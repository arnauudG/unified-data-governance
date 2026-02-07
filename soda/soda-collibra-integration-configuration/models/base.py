from pydantic import BaseModel


class BaseModelConfig(
    BaseModel,
    extra = "allow"
):
    ...