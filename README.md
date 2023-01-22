# Reactive NOSQL Database

A NOSQL database created using OOP design patterns. The patterns used were: 

Command: Creates command objects for all operations on the database. Commands can be undone to allow for transactions to be aborted. 
  
Chain of responsibility: Chain of responsibility is used to validate inputs. 
  
Observer: Used the observer patterns to create cursors which are notified when changes are made to specified data entries. 
  
Memento: Allows for a complete rollback of all data to a set save point.
  
Decorator: Created a base databse object, as well as a "PersistentDB" object. PersistentDB is a decorator for the base database object, but implements the methods for snapshotting and restoring the database.
