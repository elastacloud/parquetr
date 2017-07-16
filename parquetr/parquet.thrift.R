# i32 version
# list<SchemaElement> schema
# i64 num_rows
# list<RowGroup> row_groups
# list<KeyValue> key_value_metadata
thrift.filemetadata <- function(filehandle, pos) {
    seek(filehandle, pos)
    me <- list(
        version = readBin(filehandle, integer(), n = 1, size = 4, endian = "little")        
    )
    print(me$version)
    ## Set the name for the class
    class(me) <- append(class(me), "thrift.filemetadata")
    return(me)
}