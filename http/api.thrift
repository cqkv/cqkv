namespace go api

struct baseResp{
    1: string code
    2: string msg
}

struct Stat{}

struct GetReq {
    1: string key(api.query="key")
}

struct GetResp {
    1: baseResp baseResp

    2: string value
}

struct PutReq {
    1: string key(api.json="key")
    2: string value(api.json="value")
}

struct PutResp {
    1: baseResp baseResp
}

struct DeleteReq {
    1: string key(api.json="key")
}

struct DeleteResp {
    1: baseResp baseResp
}

struct ListKeysReq {
}

struct ListKeysResp {
    1: baseResp baseResp
    2: list<string> keys
}

struct StatReq{
}

struct StatResp{
    1: baseResp baseResp
    2: Stat stat
}

service kv {
    GetResp Get(1: GetReq req)(api.get="/cqkv")
    PutResp Put(1: PutReq req)(api.post="/cqkv")
    DeleteResp Delete(1: DeleteReq req)(api.delete="/cqkv")
    ListKeysResp ListKeys(1: ListKeysReq req)(api.get="/cqkv/keys")
    StatResp Stat(1: StatReq req)(api.get="/cqkv/stat")
}