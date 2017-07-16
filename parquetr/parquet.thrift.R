# https://github.com/apache/thrift/blob/master/lib/csharp/src/Protocol/TCompactProtocol.cs
# Need to implement zigzag and next byte for I32 based on the MSB being set
readStructBegin <- function(filehandle, pos) {
    seek(filehandle, pos)
    type <- readBin(filehandle, integer(), n = 1, size = 1, endian = "little")
    # mask off the 4 MSB of the type header. it could contain a field id delta.
    modifier <- bitwShiftR(4, bitwAnd(type, 0xf0))
    fieldType <- 0
    if (modifier == 0) {
        fieldType <- bitwAnd(type, 0x0f)
    }
    print(modifier)
    print(fieldType)
    list(fieldId <- 0 + modifier, fieldType <- fieldType)
}
zigzagToInt <- function(filehandle) {
}
readVarInt32 <- function(filehandle) {
    result <- 0
    shift <- 0
    while (TRUE) {
        b <- readBin(filehandle, integer(), size = 1, n = 1, endian = "little")
        result <- bitwShiftL(shift, bitwAnd(result, 0x7f))
        print(result)
        if (bitwAnd(b, 0x80) != 0x80) break
        shift <- shift + 7 
    }
    return(result)
}

readI16 <- function(filehandle) {
   readVarInt32(filehandle)
}

#readInt32() <- function() { }
#readInt64() <- function() { }
# add the thrift constants
PROTOCOL_ID <- 0x82
VERSION <- 1
VERSION_MASK <- 0x1f # 0001 1111
TYPE_MASK <- 0xE0 # 1110 0000
TYPE_BITS <- 0x07 # 0000 0111
TYPE_SHIFT_AMOUNT <- 5
# Add all of the thrift types
thrift.type.STOP = 0x00
thrift.type.BOOLEAN_TRUE = 0x01
thrift.type.BOOLEAN_FALSE = 0x02
thrift.type.BYTE = 0x03
thrift.type.I16 = 0x04
thrift.type.I32 = 0x05
thrift.type.I64 = 0x06
thrift.type.DOUBLE = 0x07
thrift.type.BINARY = 0x08
thrift.type.LIST = 0x09
thrift.type.SET = 0x0A
thrift.type.MAP = 0x0B
thrift.type.STRUCT = 0x0C

# i32 version
# list<SchemaElement> schema
# i64 num_rows
# list<RowGroup> row_groups
# list<KeyValue> key_value_metadata
thrift.filemetadata <- function(filehandle, pos) {
    # We read the field and mask the msb; then we read an INT16 to determine the field type
    # At the start we don't care about the ThriftCompactProtocol
    struct <- readStructBegin(filehandle, pos)
    print(struct$fieldType)
    me <- list(
        version = readI16(filehandle)
    )
    print(me$version)
    ## Set the name for the class
    class(me) <- append(class(me), "thrift.filemetadata")
    return(me)
}