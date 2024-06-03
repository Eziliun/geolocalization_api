from datetime import datetime


class QueryFormatter:

    def execute(self, result, cursor):
        try:
            formatted_result = []
            columns = [col[0] for col in cursor.description]

            for tupla in result:
                current_dictionary = {}

                for i, current_value in enumerate(tupla):
                    if isinstance(current_value, bytes):
                        current_string_value = current_value.decode('utf-8')
                        current_dictionary[columns[i]] = current_string_value.strip()

                    elif isinstance(current_value, datetime):
                        current_dictionary[columns[i]] = self.converter_datetime(current_value)

                    else:
                        current_dictionary[columns[i]] = current_value.strip() if isinstance(current_value, str) else current_value

                formatted_result.append(current_dictionary)

            return True, formatted_result

        except Exception as exc:
            self.logger.log(message=str(exc), level='error')

            return False, {
                "status": False,
                "message": str(exc),
                "result": None,
                "code": 500
            }

    @staticmethod
    def converter_datetime(obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
