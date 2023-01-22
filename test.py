import unittest
from database import *


class TestDB(unittest.TestCase):
    def setUp(self):
        self.database = BaseDB()
        self.database_decorator = PersistentDB(self.database)

    def test_basedb_put(self):
        returned_db = self.database.put('Key', 5)
        self.assertEqual(returned_db.get("Key"), 5)

    def test_basedb_put_fail(self):
        self.assertRaises(TypeError, self.database.put, 2, 5)

    def test_basedb_get(self):
        self.database.put('Key', 100)
        self.assertEqual(self.database.get('Key'), 100)

    def test_basedb_get_fail(self):
        self.assertRaises(KeyError, self.database.get, 'Non Existent Key')

    def test_basedb_get_type_fail(self):
        self.database.put("Key", 1)
        self.assertRaises(TypeError, self.database.get, 'Key', str)

    def test_basedb_get_int(self):
        self.database.put('IntKey', 2)

        self.assertEqual(self.database.get('IntKey', int), 2)

    def test_basedb_get_float(self):
        self.database.put('FloatKey', 3.5)
        self.assertEqual(self.database.get('FloatKey', float), 3.5)

    def test_basedb_get_string(self):
        self.database.put('StringKey', 'StringValue')
        self.assertEqual(self.database.get('StringKey', str), 'StringValue')

    def test_basedb_get_array(self):
        array_data = [2.3, "at", 1.67e3, [1, "me"], "bat"]
        new_array = Array.from_string(json.dumps(array_data))
        self.database.put("ArrayKey", new_array)

        self.assertEqual(self.database.get(
            'ArrayKey', Array).to_string(), new_array.to_string())

    def test_basedb_get_object(self):
        object_data = {"account 12343": {"name": "Bill",
                                         "address": "123 main street",
                                         "phones": ["619-594-3535"],
                                         "balance": 1234.05}}
        new_object = Object.from_string(json.dumps(object_data))
        self.database.put("ObjectKey", new_object)

        self.assertEqual(self.database.get(
            'ObjectKey', Object).to_string(), new_object.to_string())

    def test_basedb_remove(self):
        self.database.put('Key', 100)
        self.assertEqual(self.database.remove('Key'), 100)

    def test_basedb_remove_fail(self):
        self.assertRaises(KeyError, self.database.remove, 'Non Existent Key')

    def test_basedb_get_json(self):
        self.database.put("Key1", 500)
        self.database.put("Key2", 3.5)
        self.database.put("Key3", 'string')

        array_data = [1, 2, 3]
        new_array = Array.from_string(json.dumps(array_data))
        self.database.put("Key4", new_array)

        object_data = {"name": "Roger", "age": 21}
        new_object = Object.from_string(json.dumps(object_data))
        self.database.put("Key5", new_object)

        self.assertEqual((json.dumps({"Key1": 500, "Key2": 3.5,
                                      "Key3": "string",
                                      "Key4": [1, 2, 3],
                                      "Key5": {"name": "Roger", "age": 21}})),
                         self.database.get_json())

    def test_basedb_get_cursor(self):
        self.database.put('Key', 1)
        cursor = self.database.get_cursor('Key')
        self.database.put('Key', 2)
        self.assertEqual(2, cursor.get())

    def test_persistentdb_put(self):
        returned_db = self.database_decorator.put('Key', 5)
        self.assertEqual(returned_db.get("Key"), 5)

    def test_persistentdb_put_fail(self):
        self.assertRaises(TypeError, self.database_decorator.put, 2, 5)

    def test_persistentdb_get(self):
        self.database_decorator.put('Key', 100)
        self.assertEqual(self.database_decorator.get('Key'), 100)

    def test_persistentdb_get_fail(self):
        self.assertRaises(KeyError, self.database_decorator.get,
                          'Non Existent Key')

    def test_persistentdb_get_cursor(self):
        self.database_decorator.put('Key', 1)
        cursor = self.database_decorator.get_cursor('Key')
        self.database_decorator.put('Key', 2)
        self.assertEqual(2, cursor.get())

    def test_persistentdb_transaction(self):
        transaction = self.database_decorator.transaction()
        self.assertEqual(type(transaction), Transaction)

    def test_persistentdb_recover(self):
        command_file = 'test_commands.txt'
        snapshot_file = 'test_snapshot.txt'
        with open(command_file, 'w') as file:
            file.write(json.dumps(['PutCommand', 'Key', 1000]))
        new_obj = Object.from_string(json.dumps(
            {"account 12343": {"name": "Bill",
                               "address": "123 main street",
                               "phones": ["619-594-3535"],
                               "balance": 1234.05}}))

        self.database_decorator.put("ObjectKey", new_obj)

        with open(snapshot_file, 'w') as file:
            file.write(json.dumps(new_obj.to_string()))

        self.database_decorator.snapshot(snapshot=snapshot_file)

        recovered_database = PersistentDB.recover(command_file, snapshot_file)
        self.assertTrue(recovered_database.get('Key') == 1000 and
                        recovered_database.get('ObjectKey').to_string() ==
                        json.dumps({'account 12343': {'name': 'Bill',
                                                      'address': '123 main street',
                                                      'phones': ['619-594-3535'],
                                                      'balance': 1234.05}}))

    def test_persistentdb_snapshot(self):
        command_file = 'test_commands.txt'
        snapshot_file = 'test_snapshot.txt'

        self.database_decorator.put("Key", 1000)
        self.database_decorator.snapshot(command_file, snapshot_file)

        with open(snapshot_file) as file:
            for line in file:
                database_json = json.loads(line)
                self.assertEqual(database_json, {"Key": 1000})

    def test_put_command_execute(self):
        command = PutCommand('test_commands.txt', self.database, 'Key', 5)
        command.execute()
        self.assertEqual(self.database.get_json(), '{"Key": 5}')

    def test_put_command_undo(self):
        command = PutCommand('test_commands.txt', self.database, 'Key', 5)
        command.execute()
        command.undo()
        self.assertEqual(self.database.get_json(), '{}')

    def test_put_command_undo_replace(self):
        # undo when the put command is replacing an old value
        self.database.put('Key', 100)
        command = PutCommand('test_commands.txt', self.database, 'Key', 5)
        command.execute()
        command.undo()
        self.assertEqual(self.database.get_json(), '{"Key": 100}')

    def test_remove_command(self):
        self.database.put('Key', 100)
        command = RemoveCommand('test_commands.txt', self.database, 'Key')
        command.execute()
        self.assertEqual(self.database.get_json(), '{}')

    def test_remove_command_undo(self):
        self.database.put('Key', 100)
        command = RemoveCommand('test_commands.txt', self.database, 'Key')
        command.execute()
        command.undo()
        self.assertEqual(self.database.get_json(), '{"Key": 100}')

    def test_transaction_put(self):
        transaction = Transaction(self.database, 'test_commands.txt')
        transaction.put('Key', 5)
        self.assertEqual(self.database.get("Key"), 5)

    def test_transaction_remove(self):
        self.database.put('Key', 5)
        transaction = Transaction(self.database, 'test_commands.txt')
        transaction.remove('Key')
        self.assertEqual(self.database.get_json(), '{}')

    def test_transaction_get(self):
        self.database.put('Key', 5)
        transaction = Transaction(self.database, 'test_commands.txt')
        self.assertEqual(transaction.get('Key'), 5)

    def test_transaction_get_type(self):
        self.database.put('Key', 5)
        transaction = Transaction(self.database, 'test_commands.txt')
        self.assertEqual(transaction.get('Key', int), 5)

    def test_transaction_get_type(self):
        self.database.put('Key', 5)
        transaction = Transaction(self.database, 'test_commands.txt')
        self.assertEqual(transaction.get('Key', int), 5)

    def test_transaction_get_type(self):
        self.database.put('Key', 5)
        transaction = Transaction(self.database, 'test_commands.txt')
        self.assertEqual(transaction.get('Key', int), 5)

    def test__transaction_abort(self):
        # abort and restore the old value after aborting
        self.database.put('Key', 5)
        transaction = Transaction(self.database, 'test_commands.txt')
        transaction.put('Key', 100)
        transaction.abort()

        self.assertEqual(self.database.get('Key'), 5)

    def test_transaction_is_active(self):
        transaction = Transaction(self.database, 'test_commands.txt')
        transaction.put('Key', 100)
        transaction.commit()
        self.assertRaises(Exception, transaction.put, 'Key2', 500)

    def test_memento_save_state(self):
        test_memento = Memento("NewState", 'test_snapshot.txt')
        test_memento.save_state()

        with open('test_snapshot.txt') as file:
            state = file.read()
            self.assertEqual(state, "NewState")

    def test_cursor_get(self):
        self.database.put("Key", 1)
        cursor = self.database.get_cursor("Key")
        self.database.put("Key", 2)
        self.assertEqual(2, cursor.get("Key"))

    def test_observers(self):
        # when observer is updated, it keeps a counter of the number of changes
        self.database.put("Key", 1)
        cursor = self.database.get_cursor("Key")
        test_observer = Observer()
        cursor.add_observer(test_observer)
        self.database.put("Key", 2)
        self.database.put("Key", 1)
        self.assertEqual(test_observer.get_number_of_changes(), 2)

    def test_remove_observer(self):
        self.database.put("Key", 1)
        cursor = self.database.get_cursor("Key")
        test_observer = Observer()
        cursor.add_observer(test_observer)
        self.database.put("Key", 2)
        cursor.remove_observer(test_observer)
        self.database.put("Key", 1)
        self.assertEqual(test_observer.get_number_of_changes(), 1)

    def test_object_put(self):
        obj = Object()
        obj.put("Key", 5)
        self.assertEqual(obj.get('Key'), 5)

    def test_object_remove(self):
        obj = Object()
        obj.put("Key", 5)
        removed_value = obj.remove("Key")
        self.assertEqual(removed_value, 5)

    def test_object_length(self):
        obj = Object()
        obj.put("Key", 5)
        obj.put("Key2", 1.5)
        obj.put("Key3", 10)
        self.assertEqual(obj.length(), 3)

    def test_object_from_string(self):
        object_data = {"account 12343": {"name": "Bill",
                                         "address": "123 main street",
                                         "phones": ["619-594-3535"],
                                         "balance": 1234.05}}
        new_object = Object.from_string(json.dumps(object_data))

        self.assertEqual(new_object.to_string(), json.dumps(object_data))

    def test_object_to_string(self):
        test_object = Object()
        test_object.put('Key', 1)

        self.assertEqual(test_object.to_string(), '{"Key": 1}')

    def test_array_put(self):
        test_array = Array()
        test_array.put(5)
        self.assertEqual(test_array.get(index=0), 5)

    def test_array_remove(self):
        test_array = Array()
        test_array.put(5)
        removed_value = test_array.remove(index=0)
        self.assertEqual(removed_value, 5)

    def test_array_length(self):
        test_array = Array()
        test_array.put(5)
        test_array.put(6)
        test_array.put(7)
        self.assertEqual(test_array.length(), 3)

    def test_array_from_string(self):
        array_data = [2.3, "at", 1.67e3, [1, "me", {"a": 1}], "bat"]
        test_array = Array.from_string(json.dumps(array_data))
        self.assertEqual(test_array.to_string(),
                         json.dumps([2.3, "at", 1.67e3, [1, "me", {"a": 1}], "bat"]))

    def test_array_to_string(self):
        test_array = Array()
        test_array.put(1)
        test_array.put(2)
        self.assertEqual(test_array.to_string(), '[1, 2]')

    def test_number_validator_is_valid(self):
        validator = NumberValidator()
        self.assertTrue(validator.is_valid(5) and
                        validator.is_valid(3.5))

    def test_string_validator_is_valid(self):
        validator = StringValidator()
        self.assertTrue(validator.is_valid("string test"))

    def test_array_validator_is_valid(self):
        validator = ArrayValidator()
        array_data = [2.3, "at", 1.67e3, [1, "me", {"a": 1}], "bat"]
        test_array = Array.from_string(json.dumps(array_data))
        self.assertTrue(validator.is_valid(test_array))

    def test_object_validator_is_valid(self):
        validator = ObjectValidator()
        object_data = {"account 12343": {"name": "Bill",
                                         "address": "123 main street",
                                         "phones": ["619-594-3535"],
                                         "balance": 1234.05}}
        test_object = Object.from_string(json.dumps(object_data))
        self.assertTrue(validator.is_valid(test_object))

    def test_validator_is_valid(self):
        validator = Validator()
        self.assertTrue(validator.is_valid('String'))

    def test_validator_is_valid_fail(self):
        validator = Validator()
        self.assertFalse(validator.is_valid((3, 5)))


if __name__ == '__main__':
    unittest.main()
