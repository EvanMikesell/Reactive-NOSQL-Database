import json


class Validator():
    def __init__(self) -> None:
        """
        Base validator which creates the chain of responsibility.
        """
        self.__number_validator = NumberValidator()
        self.__string_validator = StringValidator()
        self.__array_validator = ArrayValidator()
        self.__object_validator = ObjectValidator()
        self.__invalid_type_validator = InvalidDataTypeValidator()

        self.__set_next_validator(self.__number_validator)
        self.__number_validator.set_next_validator(self.__string_validator)
        self.__string_validator.set_next_validator(self.__array_validator)
        self.__array_validator .set_next_validator(self.__object_validator)
        self.__object_validator.set_next_validator(
            self.__invalid_type_validator)

    def is_valid(self, data) -> bool:
        """
        Each validator sends the data down the chain if it is invalid.
        The final validator should return false.
        """
        return self.__next_validator.is_valid(data)

    def __set_next_validator(self, next_validator: 'Validator') -> None:
        self.__next_validator = next_validator


class ObjectValidator(Validator):
    def __init__(self) -> None:
        self.__next_validator = None

    def set_next_validator(self, next_validator) -> None:
        self.__next_validator = next_validator

    def is_valid(self, data) -> bool:
        if type(data) == Object:
            return True
        else:
            return self.__next_validator.is_valid(data)


class ArrayValidator(Validator):
    def __init__(self) -> None:
        self.__next_validator = None

    def set_next_validator(self, next_validator) -> None:
        self.__next_validator = next_validator

    def is_valid(self, data) -> bool:
        if type(data) == Array:
            return True
        else:
            return self.__next_validator.is_valid(data)


class StringValidator(Validator):
    def __init__(self) -> None:
        self.__next_validator = None

    def set_next_validator(self, next_validator) -> None:
        self.__next_validator = next_validator

    def is_valid(self, data) -> bool:
        if type(data) == str:
            return True
        else:
            return self.__next_validator.is_valid(data)


class NumberValidator(Validator):
    def __init__(self) -> None:
        self.__next_validator = None

    def set_next_validator(self, next_validator) -> None:
        self.__next_validator = next_validator

    def is_valid(self, data) -> bool:
        if type(data) == int or type(data) == float:
            return True
        else:
            return self.__next_validator.is_valid(data)


class InvalidDataTypeValidator(Validator):
    """
    Should be set as the final class in the chain of validators.
    """

    def __init__(self) -> None:
        self.__next_validator = None

    def set_next_validator(self, next_validator) -> None:
        return

    def is_valid(self, value) -> bool:
        return False


class Database:
    """
    Database interface.
    """

    def put(self, key: str, value) -> 'Database':
        pass

    def get(self, key: str, value_type=None):
        pass

    def remove(self, key: str):
        """
        Returns the removed key.
        """
        pass

    def get_json(self) -> str:
        """
        Returns a json string of the database's dictionary.
        """
        pass

    def get_cursor(self, key) -> 'Cursor':
        """
        Creates a dictionary of (key, cursor) and returns the cursor.
        """
        pass


class BaseDB(Database):
    def __init__(self) -> None:
        self.__data = dict()
        self.__validator = Validator()
        self.__cursors = dict()

    def put(self, key: str, value) -> Database:
        if type(key) != str:
            raise TypeError("Invalid Key.")
        if self.__validator.is_valid(value):
            self.__data[key] = value
            self.__update(key, value)
        else:
            raise TypeError("Invalid value type.")
        return self

    def get(self, key: str, value_type=None):
        try:
            value = self.__data[key]
        except KeyError as e:
            raise e

        if value_type:
            if type(value) != value_type:
                raise TypeError("Does not contain given type.")

        return value

    def remove(self, key: str):
        try:
            removed_value = self.__data.pop(key)
        except KeyError as e:
            raise e

        self.__update(key, None)
        return removed_value

    def get_json(self) -> str:
        # if the value is an Array or Object, turn into dictionary or list.
        for (key, value) in self.__data.items():
            if type(value) == Array or type(value) == Object:
                self.__data[key] = json.loads(value.to_string())
        return json.dumps(self.__data)

    def get_cursor(self, key: str) -> 'Cursor':
        if key in self.__data:
            if not key in self.__cursors:
                self.__cursors[key] = list()
            cursor = Cursor(self, key)
            self.__cursors[key].append(cursor)
            return cursor
        else:
            raise KeyError("Key does not exist in database.")

    def __update(self, key, updated_value) -> None:
        """
        Passes the new value to the cursor.
        New value is null if the item was removed.
        """
        if key in self.__cursors:
            for cursor in self.__cursors[key]:
                cursor.update(updated_value)


class PersistentDB(Database):
    """
    Decorator class to create commands for the database.
    Also handles snapshotting and restoring the database.
    """

    def __init__(self, database=BaseDB(), command_file='commands.txt',
                 snapshot_file='dbSnapshot.txt') -> None:
        self.__command_file = command_file
        self.__snapshot_file = snapshot_file
        self.__decorated_database = database

    def put(self, key: str, value) -> Database:
        command = PutCommand(self.__command_file, self.__decorated_database,
                             key, value)
        command.execute()
        return self

    def get(self, key: str, value_type=None):
        return self.__decorated_database.get(key, value_type)

    def remove(self, key: str):
        command = RemoveCommand(self.__command_file,
                                self.__decorated_database, key)
        return command.execute()

    def get_json(self) -> str:
        return self.__decorated_database.get_json()

    def transaction(self) -> 'Transaction':
        return Transaction(self.__decorated_database, self.__command_file)

    def snapshot(self, commands=None, snapshot=None) -> None:
        """
        Stores a snapshot of the current database in the snapshot file.
        Uses the default files if none are provided.
        """
        if commands == None:
            commands = self.__command_file
        if snapshot == None:
            snapshot = self.__snapshot_file

        database_json = self.__decorated_database.get_json()
        memento = Memento(database_json, snapshot)
        memento.save_state()

        # clear command file after snapshotting
        open(commands, 'w').close()

    @classmethod
    def recover(cls, commands=None, snapshot=None) -> 'PersistentDB':
        """
        Restore the database through the command and snapshot files.
        Gets the most recent snapshot of the database from the snapshot file.
        Then, run all the commands in order from the command file.
        Returns a persistent database.
        """
        if commands == None:
            commands = 'commands.txt'
        if snapshot == None:
            snapshot = 'dbSnapshot.txt'

        recovered_database = BaseDB()
        with open(snapshot) as snapshot_file:
            dictionary = json.load(snapshot_file)
        for data in dictionary.items():
            # data is (key, value) pair.
            if type(data[1]) == dict:
                new_obj = Object.from_string(json.dumps(data[1]))
                recovered_database.put(data[0], new_obj)
            elif type(data[1]) == list:
                new_array = Array.from_string(json.dumps(data[1]))
                recovered_database.put(data[0], new_array)
            else:
                recovered_database.put(data[0], data[1])

        # each line is a list containing command type, key, value, old value.
        with open(commands) as command_file:
            for line in command_file:
                command_vars = json.loads(line)
                command_name = command_vars.pop(0)
                command_type = globals()[command_name]

                recovered_command = command_type(commands, recovered_database,
                                                 *command_vars)
                recovered_command.execute(logging=False)

        return PersistentDB(recovered_database)

    def get_cursor(self, key: str) -> 'Cursor':
        return self.__decorated_database.get_cursor(key)


class Command():
    def __init__(self, command_file: str, database: BaseDB,
                 key: str, value) -> None:
        pass

    def execute(self, logging: bool = True):
        pass

    def undo(self) -> None:
        pass

    def __log(self) -> None:
        pass


class PutCommand(Command):
    def __init__(self, command_file: str, database: BaseDB,
                 key: str, value) -> None:
        self.__command_file = command_file
        self.__database = database
        self.__key = key
        self.__value = value

        # for 'undo' purposes, stores the old value at the given key if one existed.
        try:
            self.__old_value = self.__database.get(key)
        except KeyError:
            self.__old_value = None

    def execute(self, logging: bool = True):
        if logging:
            self.__log()
        return self.__database.put(self.__key, self.__value)

    def undo(self) -> None:
        """
        If an old value existed, you undo by adding that old value back.
        Otherwise you undo by removing the key.
        """
        if self.__old_value:
            undo_command = PutCommand(self.__command_file,
                                      self.__database, self.__key, self.__old_value)
        else:
            undo_command = RemoveCommand(self.__command_file,
                                         self.__database, self.__key)
        undo_command.execute()

    def __log(self) -> None:
        value = self.__value
        old_value = self.__old_value
        if type(self.__value) == Array or type(self.__value) == Object:
            value = self.__value.to_string()
        if type(self.__old_value) == Array or type(self.__old_value) == Object:
            old_value = self.__old_value.to_string()

        command_list = ['PutCommand', self.__key, value]

        # if the key had a previous value, record the old value
        if self.__old_value:
            command_list.append(old_value)

        with open(self.__command_file, 'a') as commands_file:
            commands_file.write(json.dumps(command_list) + '\n')


class RemoveCommand(Command):
    def __init__(self, command_file: str, database: BaseDB,
                 key: str) -> None:
        self.__command_file = command_file
        self.__database = database
        self.__key = key

        # in order to 'undo', stores the old value at the given key if one existed.
        try:
            self.__old_value = self.__database.get(key)
        except Exception:
            self.__old_value = None

    def execute(self, logging: bool = True):
        if logging:
            self.__log()
        return self.__database.remove(self.__key)

    def undo(self) -> None:
        """
        Undo a remove by putting the old value back.
        """
        undo_command = PutCommand(
            self.__command_file, self.__database, self.__key, self.__old_value)
        undo_command.execute()

    def __log(self) -> None:
        old_value = self.__old_value
        if type(self.__old_value) == Array or type(self.__old_value) == Object:
            old_value = self.__old_value.to_string()
        command_list = ['RemoveCommand',  str(self.__key)]

        # if the key had a previous value, record the old value
        if self.__old_value:
            command_list.append(old_value)

        with open(self.__command_file, 'a') as commands_file:
            commands_file.write(json.dumps(command_list) + '\n')


class Transaction():
    """
    Records commands in order to undo them if the transaction is aborted.
    """

    def __init__(self, database: Database, command_file) -> None:
        self.__database = database
        self.__command_file = command_file
        self.__commands = []
        self.__is_active = True

    def put(self, key: str, value) -> Database:
        if not self.__is_active:
            raise Exception("Inactive Transaction")

        command = PutCommand(self.__command_file, self.__database, key, value)
        self.__commands.append(command)
        return command.execute()

    def get(self, key: str, value_type=None):
        if not self.__is_active:
            raise Exception("Inactive Transaction")

        return self.__database.get(key, value_type)

    def remove(self, key: str):
        if not self.__is_active:
            raise Exception("Inactive Transaction")

        command = RemoveCommand(self.__command_file, self.__database, key)
        self.__commands.append(command)
        return command.execute()

    def commit(self) -> None:
        if not self.__is_active:
            raise Exception("Inactive Transaction")

        self.__is_active = False

    def abort(self) -> None:
        if not self.__is_active:
            raise Exception("Inactive Transaction")

        # reversed order, the most recent commands need to be undone first
        for command in reversed(self.__commands):
            command.undo()
        self.__is_active = False


class Memento():
    def __init__(self, state, file) -> None:
        self.__state = state
        self.__file = file

    def save_state(self) -> None:
        with open(self.__file, 'w') as f:
            f.write(self.__state)


class Array:
    def __init__(self) -> None:
        self.__list = list()
        self.__validator = Validator()

    def put(self, value) -> 'Array':
        if self.__validator.is_valid(value):
            self.__list.append(value)
        return self

    def get(self, index: int, value_type=None):
        value = self.__list[index]
        if value_type:
            if type(value) != value_type:
                raise TypeError("Does not contain given type")
        return value

    def length(self) -> int:
        return len(self.__list)

    def to_string(self) -> str:
        """
        Recursively calls to_string when we run into an Array or Object
        """
        for element in self.__list:
            if type(element) == Array or type(element) == Object:
                self.__list[self.__list.index(element)] = \
                    json.loads(element.to_string())

        return json.dumps(self.__list)

    def remove(self, index: int):
        try:
            return self.__list.pop(index)
        except IndexError:
            return None

    @classmethod
    def from_string(cls, array_json: str) -> 'Array':
        """
        Turn the string into a list.
        Then, create a new Array by putting each list element into an array.
        """
        try:
            array_data = json.loads(array_json)
        except Exception as e:
            raise e

        new_array = Array()

        # dictionaries and lists become arrays and objects
        for element in array_data:
            if type(element) == dict:
                new_array.put(Object.from_string(json.dumps(element)))
            elif type(element) == list:
                new_array.put(Array.from_string(json.dumps(element)))
            else:
                new_array.put(element)
        return new_array


class Object:
    def __init__(self) -> None:
        self.__data = dict()
        self.__validator = Validator()

    def put(self, key: str, value) -> 'Object':
        if self.__validator.is_valid(value) and type(key) == str:
            self.__data[key] = value
        return self

    def get(self, key: str, value_type=None):
        value = self.__data[key]
        if value_type:
            if type(value) != value_type:
                raise TypeError("Does not contain given type")
        return value

    def length(self) -> int:
        return len(self.__data)

    def to_string(self) -> str:
        """
        Recursively calls to_string when we run into an Array or Object
        """
        for (key, value) in self.__data.items():
            if type(value) == Array or type(value) == Object:
                self.__data[key] = json.loads(value.to_string())

        return json.dumps(self.__data)

    def remove(self, key: str):
        return self.__data.pop(key)

    @classmethod
    def from_string(cls, object_json: str) -> 'Object':
        """
        Turn the string into a dictionary, then add each value from the 
        dictionary into the new object.
        """
        try:
            object_data = json.loads(object_json)
        except Exception as e:
            raise e

        new_object = Object()

        # dictionaries and lists become arrays and objects
        for (key, value) in object_data.items():
            if type(key) != str:
                raise TypeError

            if type(value) == dict:
                new_object.put(key, Object.from_string(json.dumps(value)))
            elif type(value) == list:
                new_object.put(key, Array.from_string(json.dumps(value)))
            else:
                new_object.put(key, value)

        return new_object


class Observer():
    def __init__(self) -> None:
        self.__changes = 0

    def update(self, updated_value) -> None:
        self.__changes += 1

    def get_number_of_changes(self) -> int:
        return self.__changes


class Cursor():
    """
    Holds a value from the database. 
    Cursor is notified when this value is updated.
    Can hold observers which will be notified when the value is updated.
    """

    def __init__(self,  database: BaseDB, key: str) -> None:
        self.__key = key
        self.__database = database
        self.__observers = list()

    def get(self, value_type=None):
        if value_type:
            return self.__database.get(self.__key)
        else:
            return self.__database.get(self.__key, value_type)

    def add_observer(self, o: Observer) -> None:
        self.__observers.append(o)

    def remove_observer(self, o: Observer) -> None:
        self.__observers.remove(o)

    def update(self, updated_value) -> None:
        for observer in self.__observers:
            observer.update(updated_value)
