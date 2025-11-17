from pipeline.models.inspection_result import ExtractedInspectionResult, InspectionResultModel

class Utils:
    @staticmethod
    def extract_number_from_amount_string(amount_string: str) -> float:
        """
        Params
        ------
            amount_string: str
                Ex:
                - convert '$40,000.00 ' into 40000.0

        Returns:
            float: extracted amount
        """

        replacements = str.maketrans({"$": "", " ": "", ",": ""})
        return amount_string.translate(replacements)