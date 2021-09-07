from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A


class MemberClaimsIdentifiers:
    @staticmethod
    def get_explanation_of_benefit_id() -> AutoMapperTextLikeBase:
        return A.text("1")

    @staticmethod
    def get_patient_id() -> AutoMapperTextLikeBase:
        return A.column("MEMBER_ID")
