import unittest

from person_utils import PersonInputError, format_single_result, parse_person_request


class PersonUtilsTests(unittest.TestCase):
    def test_parses_labeled_multiline_message(self):
        self.assertEqual(
            parse_person_request("ФИО: Иванов Иван\nИИН: 123 456 789 012"),
            ("Иванов Иван", "123456789012"),
        )

    def test_rejects_missing_iin(self):
        with self.assertRaises(PersonInputError):
            parse_person_request("Иванов Иван")

    def test_formats_debt_details(self):
        rendered = format_single_result({
            "fio": "Иванов Иван",
            "iin": "123456789012",
            "check_status": "Обработано",
            "travel_status": "Разрешен",
            "total_amount": "100",
            "debts_count": 1,
            "details": [{"issuer": "Орган", "amount": "100", "start_date": "01.01.2026"}],
        })
        self.assertIn("1. Орган | 100 | 01.01.2026", rendered)


if __name__ == "__main__":
    unittest.main()
