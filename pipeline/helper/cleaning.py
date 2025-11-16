from datetime import datetime
from typing import Union

from pipeline.constants import Constants
from pipeline.helper.common.utils import Utils
from pipeline.model.inspection_result import InspectionResultModel

class DataCleaningService:
    def _is_value_null(self, value: Union[int, str, None]) -> bool:
        """
        Params:
            value (int or str or None): value to inspect

        Returns:
            boolean: True if given value is none
        """
        return value is None

    def _generate_inspection_results(
        self,
        actual_value: Union[str, int, None], 
        verify_reasons: Union[str, None] = None,
        new_value_to_assign: Union[str, int, None] = None
    ) -> InspectionResultModel:
        """
        Params
        ------
            field_to_inspect: str 
                field name to inspect
            actual_value: str or int or None
                actual value from the field
            verify_reasons: str or None, optional
                reasons why further verification needed
            new_value_to_assign: str or int or None, optional
                new value to assign

        Returns:
            InspectionResultModel: inspection results model
        """
        response_dict = {}

        if verify_reasons is None:
            response_dict["needs_to_verify"] = False
        else:
            response_dict["needs_to_verify"] = True
            response_dict["verify_reasons"] = verify_reasons
        
        response_dict["actual_value"] = actual_value

        if new_value_to_assign is not None:
            response_dict["value_to_assign"] = new_value_to_assign

        return self.InspectionResultModel(**response_dict)
    
    def inspect_loan_id(self, loan_id: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            loan_id (str or None): loan id

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(loan_id):
            return self._generate_inspection_results(loan_id, f"missing value on {Constants.LOAN_ID}")
        return self._generate_inspection_results(loan_id)

    def inspect_debtor_name(self, name: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            name (str or None): debtor name

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(name):
            return self._generate_inspection_results(name, f"missing value on {Constants.DEBTOR_NAME}")
        
        return self._generate_inspection_results(name)
        
    def inspect_zip(self, zip_code: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            zip_code (str or None): zip code

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(zip_code):
            return self._generate_inspection_results(zip_code, f"missing value on {Constants.DEBTOR_ORIGIN_ZIP_CODE}")
        else:
            if len(zip_code) <= 3:
                return self._generate_inspection_results(zip, f"invalid {Constants.DEBTOR_ORIGIN_ZIP_CODE}", "invalid")

        return self._generate_inspection_results(zip)

    def inspect_city(self, city: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            city (str or None): debtor's origin city name
        
        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(city):
            return self._generate_inspection_results(city, f"missing value on {Constants.DEBTOR_ORIGIN_CITY}")
        else:
            if len(city) <= 2:
                return self._generate_inspection_results(city, f"invalid {Constants.DEBTOR_ORIGIN_CITY}", "invalid")

        return self._generate_inspection_results(city)

    def inspect_bank_name(self, bank_name: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            bank_name (str or None): loan's bank guarantor

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(bank_name):
            return self._generate_inspection_results(bank_name, f"missing value on {Constants.GUARANTOR_BANK_NAME}")
        return self._generate_inspection_results(bank_name)
    
    def inspect_bank_state(self, bank_state: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            bank_state (str or None): bank guarantor's name

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(bank_state):
            return self._generate_inspection_results(bank_state, f"missing value on {Constants.GUARANTOR_BANK_STATE}")
        return self._generate_inspection_results(bank_state)
    
    def inspect_naics(self, naics: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            naics (str or None): NAICS code

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(naics):
            return self._generate_inspection_results(naics, f"missing value on {Constants.NAICS_CODE}")
        else:
            if len(naics) < 6:
                return self._generate_inspection_results(naics, f"invalid {Constants.NAICS_CODE}", "invalid")

        return self._generate_inspection_results(naics)

    def inspect_approval_date(self, approval_date: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            approval_date (str or None): loan's approval date

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(approval_date):
            return self._generate_inspection_results(approval_date, f"missing value on {Constants.LOAN_APPROVAL_DATE}")

        formatted_date = datetime.strptime(approval_date, Constants.RAW_DATE_FORMAT)
        formatted_date_string = datetime.strftime(formatted_date, Constants.CLEAN_DATE_FORMAT)
        
        return self._generate_inspection_results(approval_date, new_value_to_assign=formatted_date_string)

    def inspect_approval_fiscal_year(
        self, 
        approval_date_str: Union[str, None], 
        fiscal_year: Union[int, None]
    ) -> InspectionResultModel:
        """
        Params:
            approval_date_str (str or None): approval date string
            fiscal_year (int or None): fiscal year of the approval

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(fiscal_year):
            if (approval_date_str):
                approval_date = datetime.strptime(approval_date_str, format = Constants.CLEAN_DATE_FORMAT)
                month_of_date = approval_date.month
                year_of_date = approval_date.year
                new_fiscal_year = None

                # Fiscal year of Y1998 
                # defined in Oct, 1997 through Sep, 1998
                if (month_of_date >= 10):
                    new_fiscal_year = year_of_date - 1
                else:
                    new_fiscal_year = year_of_date
            return self._generate_inspection_results(
                fiscal_year, 
                f"missing value on {Constants.LOAN_APPROVAL_FY}", 
                new_fiscal_year
            )
        else:
            return self._generate_inspection_results(fiscal_year)
        
    def inspect_term_period(self, term: Union[int, None]) -> InspectionResultModel:
        """
        Params:
            term (int or None): credit's term period in month

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(term):
            return self._generate_inspection_results(term, f"missing value on {Constants.TERM_DURATION}")
        else:
            if term == 0:
                return self._generate_inspection_results(term, f"has 0 {Constants.TERM_DURATION}")
            return self._generate_inspection_results(term)

    def inspect_no_emp(self, employee_number: Union[int, None]) -> InspectionResultModel:
        """
        Params:
            employee_number (int or None): number of existing debtor's employee

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(employee_number):
            return self._generate_inspection_results(employee_number, f"missing value on {Constants.DEBTOR_EMPLOYEE_NUMBER}")
        return self._generate_inspection_results(employee_number)
    
    def inspect_new_exist_bussiness(self, new_or_exist: Union[bool, None]) -> InspectionResultModel:
        """
        Params:
            new_or_exist (bool or None): whether the debtor is a new or existing bussiness while appling for the sba loan

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(new_or_exist):
            return self._generate_inspection_results(new_or_exist, f"missing value on {Constants.DEBTOR_NEW_OR_EXIST}")
        return self._generate_inspection_results(new_or_exist)
    
    def inspect_number_new_job_created(self, num_new_job_created: Union[int, None]) -> InspectionResultModel:
        """
        Params:
            num_new_job_created (int or None): number of job created if the loan approved

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(num_new_job_created):
            return self._generate_inspection_results(
                num_new_job_created,
                f"missing value on {Constants.NUMBER_NEW_JOB_CREATED}"
            )
        return self._generate_inspection_results(num_new_job_created)
    
    def inspect_number_job_reatined(self, num_job_retained: Union[int, None]) -> InspectionResultModel:
        """
        Params:
            num_job_retained (int or None): number of job retained if the loan approved

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(num_job_retained):
            return self._generate_inspection_results(
                num_job_retained,
                f"missing value on {Constants.NUMBER_JOB_RETAINED}"
            )
        return self._generate_inspection_results(num_job_retained)
    
    def inspect_franchise_code(self, franchise_code: Union[int, None]) -> InspectionResultModel:
        """
        If the franchise_code = 0 or 1, then the debtor is not a franchise business
        Params:
            franchise_code (int or None): debtor's franchise code

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(franchise_code):
            return self._generate_inspection_results(franchise_code, f"missing value on {Constants.DEBTOR_FRANCHISE_CODE}")
        else:
            if (franchise_code == 0 or franchise_code == 1):
                return self._generate_inspection_results(
                    franchise_code,
                    f"doesn't have {Constants.DEBTOR_FRANCHISE_CODE}",
                    None
                )
            return self._generate_inspection_results(franchise_code)
    
    def inspect_urban_rural_code(self, urban_rural_code: Union[int, None]) -> InspectionResultModel:
        """
        Params
        ------
            urban_rural_code: int or None
                - 0: undefined
                - 1: urban
                - 2: rural
        
        Returns: 
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(urban_rural_code):
            return self._generate_inspection_results(
                urban_rural_code,
                f"missing value on {Constants.DEBTOR_URBAN_RURAL_INFO}"
            )
        else:
            if urban_rural_code == 0:
                return self._generate_inspection_results(urban_rural_code, "undefined")
            elif urban_rural_code == 1:
                return self._generate_inspection_results(urban_rural_code, "urban")
            return self._generate_inspection_results(urban_rural_code, "rural")
    
    def inspect_rev_line_credit(self, rev_line_credit_code: Union[str, None]) -> InspectionResultModel:
        """
        Params
        ------
            rev_line_credit_code: str or None 
                revolving line of credit code
                - Y = revolving credit
                - N = non-revolving credit

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(rev_line_credit_code):
            return self._generate_inspection_results(
                rev_line_credit_code,
                f"missing value on {Constants.REV_LINE_CREDIT}"
                "invalid"
            )
        else:
            if rev_line_credit_code.upper() in Constants.VALID_REVOLVING_CREDIT_CODES:
                return self._generate_inspection_results(rev_line_credit_code)
            return self._generate_inspection_results(
                rev_line_credit_code,
                f"invalid {Constants.REV_LINE_CREDIT}",
                "invalid"
            )
        
    def inspect_low_doc(self, low_doc_code: Union[str, None]) -> InspectionResultModel:
        """
        Params
        ------
            low_doc_code: str or None
                low doc program code
                - Y: the loan applied through low doc program
                - N: otherwise

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(low_doc_code):
            return self._generate_inspection_results(
                low_doc_code,
                f"missing value on {Constants.LOW_DOC_PROGRAM}",
                "invalid"
            )
        else:
            if low_doc_code.upper() in Constants.VALID_LOW_DOC_CODES:
                return self._generate_inspection_results(low_doc_code)
            return self._generate_inspection_results(
                low_doc_code,
                f"invalid {Constants.LOW_DOC_PROGRAM}",
                "invalid"
            )
        
    def inspect_charge_off_date(self, charge_off_date_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            charge_off_date_str (str or None): charge off date

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(charge_off_date_str):
            return self._generate_inspection_results(
                charge_off_date_str,
                f"missing value on {Constants.CHARGED_OFF_DATE}"
            )
        
        formatted_date = datetime.strptime(charge_off_date_str, Constants.RAW_DATE_FORMAT)
        formatted_date_string = datetime.strftime(formatted_date, Constants.CLEAN_DATE_FORMAT)

        return self._generate_inspection_results(charge_off_date_str, formatted_date_string)
    
    def inspect_disbursement_date(self, disbursement_date_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            disbursement_date_str (str or None): disbursement date

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(disbursement_date_str):
            return self._generate_inspection_results(
                disbursement_date_str,
                f"missing value on {Constants.DISBURSEMENT_DATE}"
            )
        
        formatted_date = datetime.strptime(disbursement_date_str, Constants.RAW_DATE_FORMAT)
        formatted_date_string = datetime.strftime(formatted_date, Constants.CLEAN_DATE_FORMAT)

        return self._generate_inspection_results(disbursement_date_str, formatted_date_string)
    
    def inspect_disbursement_gross(self, disbursement_gross_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            disbursement_gross_str (str or None): string amount of loan disbursed

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(disbursement_gross_str):
            return self._generate_inspection_results(
                disbursement_gross_str,
                f"missing value on {Constants.DISBURESEMENT_GROSS}"
            )
        
        extracted_amount = Utils.extract_number_from_amount_string(disbursement_gross_str)
        return self._generate_inspection_results(disbursement_gross_str, extracted_amount)
    
    def inspect_balance_gross(self, balance_gross_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            balance_gross_str (str or None): string amount of balance gross

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(balance_gross_str):
            return self._generate_inspection_results(
                balance_gross_str,
                f"missing value on {Constants.OUTSTANDING_BALANCE}"
            )
        
        extracted_amount = Utils.extract_number_from_amount_string(balance_gross_str)
        return self._generate_inspection_results(balance_gross_str, extracted_amount)
    
    def inspect_charged_off_amount(self, charge_off_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            charge_off_str (str or None): string amount of charge off

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(charge_off_str):
            return self._generate_inspection_results(
                charge_off_str,
                f"missing value on {Constants.CREDIT_CHARGED_OFF_AMOUNT}"
            )
        
        extracted_amount = Utils.extract_number_from_amount_string(charge_off_str)
        return self._generate_inspection_results(charge_off_str, extracted_amount)
    
    def inspect_bank_loan_approved(self, loan_approved_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            loan_approved_str (str or None): string amount of loan approved by Bank

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(loan_approved_str):
            return self._generate_inspection_results(
                loan_approved_str,
                f"missing value on {Constants.BANK_APPROVED_CREDIT_AMOUNT}"
            )
        
        extracted_amount = Utils.extract_number_from_amount_string(loan_approved_str)
        return self._generate_inspection_results(loan_approved_str, extracted_amount)
    
    def inspect_sba_loan_approved(self, loan_approved_str: Union[str, None]) -> InspectionResultModel:
        """
        Params:
            loan_approved_str (str or None): string amount of loan approved by SBA

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(loan_approved_str):
            return self._generate_inspection_results(
                loan_approved_str,
                f"missing value on {Constants.SBA_APPROVED_CREDIT_AMOUNT}"
            )
        
        extracted_amount = Utils.extract_number_from_amount_string(loan_approved_str)
        return self._generate_inspection_results(loan_approved_str, extracted_amount)
    
    def inspect_loan_status(self, loan_status: Union[str, None]) -> InspectionResultModel:
        """
        Params
        ------
            loan_status: str or None
                - P I F: paid in full
                - CHGOFF: charged off

        Returns:
            InspectionResultModel: inspection results model
        """

        if self._is_value_null(loan_status):
            return self._generate_inspection_results(loan_status, f"missing value on {Constants.LOAN_STATUS}")
        
        formatted_loan_status = loan_status
        if formatted_loan_status.lower() == "p i f":
            formatted_loan_status = "PIF"

        return self._generate_inspection_results(loan_status, formatted_loan_status)