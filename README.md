# richsql

**Please note:** Use the **development** branch for now.

## Example

    val users : User =
        connection.map(sql"""
            select id, name, email
            from "User"
            where id = ${userId}
        """) { row => 
            row.getObject[User].get 
        }.head
  
The interface is *somewhat* typesafe. The program will only compile when your placeholders, eg. `${userId}`, have a type that can be automatically converted to an SQL compatible value.

It's possible to extract individual fields, eg. `row.getString("name")`, or whole case classes. In the above example, the `User` type could be defined as:

    case class User(id : Long, name : String, email : Option[String])
    
There is also a wrapper for the `Long` type, since that's a very commonly used type for auto increasing primary keys. It's called `Id[T]`. The `T` type is meant to be the type of a row in the table in which `Id[T]` is a primary key. For a `User`, that would be `Id[User]`, eg.

    case class User(id : Id[User], name : String, email : Option[String])
