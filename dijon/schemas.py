from pydantic import AnyHttpUrl, BaseModel, constr


# Root Server
#
#
class RootServerBase(BaseModel):
    name: constr(min_length=1, max_length=255)
    url: AnyHttpUrl

    class Config:
        orm_mode = True


class RootServer(RootServerBase):
    id: int


class RootServerCreate(RootServerBase):
    pass
