package org.foundator.richsql

import java.io.InputStream
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.sql._
import scala.Array
import scala.collection.mutable.ListBuffer
import org.joda.time.{Instant, ReadableInstant}
import org.intellij.lang.annotations.Language
import scala.reflect.runtime.universe._

object RichSql {

    private val debug = false
    private val debugMap = new ConcurrentHashMap[String, AtomicLong]()

    @inline def debugWrapper[A](statement : => String, body : => A) : A = {
        if(!debug) return body
        val sql = statement
        val before = System.currentTimeMillis()
        val result = body
        val elapsed = System.currentTimeMillis() - before
        val atomic = new AtomicLong()
        val total = Option(debugMap.putIfAbsent(sql, atomic)).getOrElse(atomic).addAndGet(elapsed)
        println(elapsed + " ms (" + total + " ms total) " + sql)
        result
    }

    def map[T](statement : PreparedStatement, parameters : Value*)(mapper : Row => T) : List[T] = {
        val list = new ListBuffer[T]()
        each(statement, parameters : _*) { row =>
            list.append(mapper(row))
        }
        list.toList
    }

    def each(statement : PreparedStatement, parameters : Value*)(body : Row => Unit) {
        statement.setFetchSize(10000)
        val sql = statement.toString
        try {
            instantiateStatement(statement, parameters : _*)
            val resultSet = debugWrapper(sql, statement.executeQuery())
            try {
                while(resultSet.next()) {
                    body(new Row(resultSet))
                }
            } finally {
                resultSet.close()
            }
        } finally {
            statement.close()
        }
    }

    def count(statement : PreparedStatement, parameters : Value*) : Int = {
        val sql = statement.toString
        try {
            instantiateStatement(statement, parameters : _*)
            debugWrapper(sql, statement.executeUpdate())
        } finally {
            statement.close()
        }
    }

    def updateOne(statement : PreparedStatement, parameters : Value*) : Unit = {
        val rowCount = count(statement, parameters : _*)
        if(rowCount != 1) throw new RuntimeException(s"SQL update failed. Expected exactly 1 row to be updated. Actual row count was $rowCount")
    }

    def batch(statement : PreparedStatement) (useBatch : Batch => Unit) : Array[Int] = {
        try {
            val batch = new Batch(statement)
            try {
                useBatch(batch)
                statement.executeBatch()
            } catch {
                case e : BatchUpdateException =>
                    var current : SQLException = e
                    var i = 1
                    while (current.getNextException != null && i < 10) {
                        current = current.getNextException
                        println(s"Batch getNextException $i")
                        current.printStackTrace()
                        i += 1
                    }
                    throw e
            }
        } finally {
            statement.close()
        }
    }

    class Batch (statement : PreparedStatement) {
        def apply(parameters : Value*) = {
            instantiateStatement(statement, parameters : _*)
            statement.addBatch()
            statement.clearParameters()
        }
    }

    def instantiateStatement(statement : PreparedStatement, parameters : Value*) : Unit = {
        // WARNING: The setTimestamp logic here assumes that the database is using the UTC timezone
        for((p, i) <- parameters.zipWithIndex) p.value match {
            case (o : ReadableInstant, _) => statement.setTimestamp(i + 1, new Timestamp(o.getMillis))
            case (o : Array[Byte], ArrayType("byte")) =>
                statement.setObject(i + 1, o, Types.BINARY)
            case (o : Array[AnyRef], ArrayType(typeString@"text")) =>
                val sqlArray = statement.getConnection.createArrayOf(typeString, o)
                statement.setArray(i + 1, sqlArray)
            case (o : Array[Long], ArrayType(typeString@"bigint")) =>
                val longArray : Array[AnyRef] = o.map(v => long2Long(v) : AnyRef)
                val sqlArray = statement.getConnection.createArrayOf(typeString, longArray)
                statement.setArray(i + 1, sqlArray)
            case (o, ScalarType(jdbcType)) => statement.setObject(i + 1, o, jdbcType)
            case o : ReadableInstant => statement.setTimestamp(i + 1, new Timestamp(o.getMillis))
            case p@(o, t) => throw new RuntimeException(s"Error in RichSql, bad value pair $p of type (${o.getClass}, ${t.getClass})")
            case o => statement.setObject(i + 1, o)
        }
    }

    class Value(val value : Any) extends AnyVal

    sealed abstract class Type[T]
    case class ScalarType[T](jdbcType : Int) extends Type[T]
    case class ArrayType[T](postgresqlType : String) extends Type[Array[T]]

    class ElementType[T](val value : String) extends AnyVal

    case class SqlLiteral(sql : String, parameters : Seq[Value])

    object Implicits {

        implicit class SqlInterpolator(@Language("SQL") val sc : StringContext) extends AnyVal {
            def sql(arguments : Value*) : SqlLiteral = {
                val strings = sc.parts.iterator
                val expressions = arguments.iterator
                val buffer = new StringBuffer(strings.next)
                while(strings.hasNext || expressions.hasNext) {
                    if(expressions.hasNext) { buffer.append("?"); expressions.next }
                    if(strings.hasNext) buffer.append(strings.next)
                }
                SqlLiteral(buffer.toString, arguments)
            }
        }

        implicit def booleanValue(value : Boolean) : Value = new Value(value)
        implicit def stringValue(value : String) : Value = new Value(value)
        implicit def intValue(value : Int) : Value = new Value(value)
        implicit def longValue(value : Long) : Value = new Value(value)
        implicit def doubleValue(value : Double) : Value = new Value(value)
        implicit def timestampValue(value : Timestamp) : Value = new Value(value)
        implicit def instantValue(value : ReadableInstant) : Value = new Value(value)
        implicit def uuidValue(value : UUID) : Value = new Value(value)
        implicit def arrayValue[T](value : Array[T])(implicit toElementType : ArrayType[T]) : Value = new Value(value -> toElementType)

        implicit def booleanType = ScalarType[Boolean](Types.BOOLEAN)
        implicit def stringType = ScalarType[String](Types.VARCHAR)
        implicit def intType = ScalarType[Int](Types.INTEGER)
        implicit def longType = ScalarType[Long](Types.BIGINT)
        implicit def doubleType = ScalarType[Double](Types.DOUBLE)
        implicit def timestampType = ScalarType[Timestamp](Types.TIMESTAMP)
        implicit def instantType = ScalarType[Instant](Types.TIMESTAMP)
        implicit def readableInstantType = ScalarType[ReadableInstant](Types.TIMESTAMP)
        implicit def stringArrayType = ArrayType[String]("text")
        implicit def longArrayType = ArrayType[Long]("bigint")
        implicit def byteArrayType = ArrayType[Byte]("byte") // Note "byte" is not a Postgres type but our own.

        def nullable[T](option : Option[T])(implicit toValue : T => Value, toType : Type[T]) = {
            new Value(option.map(v => toValue(v).value).orNull -> toType)
        }



        implicit class RichConnection(connection : Connection) {
            /** Takes all the results */
            def map[T](
                @Language("SQL")
                statement : String,
                parameters : Value*
            )(mapper : Row => T) : List[T] = {
                RichSql.map(connection.prepareStatement(statement), parameters : _*)(mapper)
            }

            /** Counts the number of changed rows */
            def count(
                @Language("SQL")
                statement : String,
                parameters : Value*
            ) : Int = {
                RichSql.count(connection.prepareStatement(statement), parameters : _*)
            }

            /** Loops over each result */
            def each(
                @Language("SQL")
                statement : String,
                parameters : Value*
            )(mapper : Row => Unit) : Unit = {
                RichSql.each(connection.prepareStatement(statement), parameters : _*)(mapper)
            }

            /** Batches the statement, so it can be called multiple times */
            def batch(
                @Language("SQL")
                statement : String,
                parameters : Value*
            )(useBatch : Batch => Unit) : Array[Int] = {
                RichSql.batch(connection.prepareStatement(statement))(useBatch)
            }

            def map[T](sql : SqlLiteral)(mapper : Row => T) : List[T] = map(sql.sql, sql.parameters : _*)(mapper)
            def count[T](sql : SqlLiteral) : Int = count(sql.sql, sql.parameters : _*)
            def each[T](sql : SqlLiteral)(mapper : Row => Unit) : Unit = each(sql.sql, sql.parameters : _*)(mapper)
            def batch[T](sql : SqlLiteral)(useBatch : Batch => Unit) : Unit = batch(sql.sql, sql.parameters : _*)(useBatch)
        }
    }

    class Row(val resultSet : ResultSet) {
        def get[T](f : ResultSet => T) = Option(f(resultSet)).filter(_ => !resultSet.wasNull())

        def getBoolean(columnLabel: String): Option[Boolean] = get(_.getBoolean(columnLabel))

        def getUuid(columnLabel: String): Option[UUID] = get(r => r.getString(columnLabel)).map(UUID.fromString)

        def getString(columnLabel: String): Option[String] = get(_.getString(columnLabel))

        def getInt(columnLabel: String): Option[Int] = get(_.getInt(columnLabel))

        def getLong(columnLabel: String): Option[Long] = get(_.getLong(columnLabel))

        def getDouble(columnLabel: String): Option[Double] = get(_.getDouble(columnLabel))

        def getTimestamp(columnLabel: String): Option[Timestamp] = get(_.getTimestamp(columnLabel))

        def getInstant(columnLabel: String): Option[Instant] = {
            get(_.getTimestamp(columnLabel)).map{ timestamp =>
                new Instant(timestamp.getTime)
            }
        }

        def getStringArray(columnLabel: String): Option[Array[String]] = {
            get(_.getArray(columnLabel)).map(_.getArray.asInstanceOf[Array[String]])
        }

        def getInputStream(columnLabel: String): Option[InputStream] = {
            get(_.getBinaryStream(columnLabel))
        }

        def getObject[T <: Product : TypeTag] : Option[T] = getPrefixedObject[T]("")

        def getPrefixedObject[T <: Product : TypeTag](prefix : String) : Option[T] = {
            val companion = typeOf[T].companion
            val apply = companion.member(TermName("apply")).asMethod
            val fields = for {
                parameter <- apply.paramLists.head
            } yield {
                val (fieldType, optional) = if(parameter.typeSignature.erasure == typeOf[Option[_]].erasure) {
                    parameter.typeSignature.typeArgs.head -> true
                } else {
                    parameter.typeSignature -> false
                }
                val name = parameter.name.toString
                val prefixed = if(prefix.nonEmpty) prefix + name.capitalize else name
                prefixed -> (fieldType, optional)
            }
            val values = for((fieldName, (fieldType, optional)) <- fields) yield {
                val option =
                    if(fieldType == typeOf[List[String]]) getStringArray(fieldName).map(_.toList)
                    else if(fieldType == typeOf[Seq[String]]) getStringArray(fieldName).map(_.toSeq)
                    else if(fieldType == typeOf[Set[String]]) getStringArray(fieldName).map(_.toSet)
                    else if(fieldType == typeOf[Array[String]]) getStringArray(fieldName)
                    else if(fieldType == typeOf[Boolean]) getBoolean(fieldName)
                    else if(fieldType == typeOf[UUID]) getUuid(fieldName)
                    else if(fieldType == typeOf[String]) getString(fieldName)
                    else if(fieldType == typeOf[Int]) getInt(fieldName)
                    else if(fieldType == typeOf[Long]) getLong(fieldName)
                    else if(fieldType == typeOf[Double]) getDouble(fieldName)
                    else if(fieldType == typeOf[Timestamp]) getTimestamp(fieldName)
                    else if(fieldType == typeOf[Instant]) getInstant(fieldName)
                    else throw new RuntimeException("Unsupported object field: " + fieldName + " : " + fieldType)
                if(optional) {
                    option
                } else {
                    if(option.isEmpty) return None
                    option.get
                }
            }
            val mirror = runtimeMirror(getClass.getClassLoader)
            val companionClass = mirror.runtimeClass(companion)
            val companionInstance = companionClass.newInstance()
            val applyMethod = companionClass.getMethods.find(_.getName == "apply").get
            val result = applyMethod.invoke(companionInstance, values.map(_.asInstanceOf[AnyRef]) : _*)
            Some(result.asInstanceOf[T])
        }
    }
}
