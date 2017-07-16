# start with the base function for reading parquet
validateFile <- function(filehandle, size) {
    header = readChar(filehandle, 4)
    seek(filehandle, size - 4)
    tail = readChar(filehandle, 4)
    header == "PAR1" & tail == "PAR1"
}
# Get the file metadata
fileMetaData <- function(filehandle, size) {
    seek(filehandle, size - 8)
    footerLength <- readBin(filehandle, integer(), n = 1, size = 4, endian = "little")
    pos <- size - 8 - footerLength
    print(pos)
    thrift.filemetadata(filehandle, pos)$version
}

# Used to open a parquet file
read.parquet <- function(filename) {
    size <- file.info(filename)$size
    
    parquetFile <- file(filename, 'rb')
    if (!validateFile(parquetFile, size)) {
        stop("Error: not a valid parquet file")
    }
    metadata <- fileMetaData(parquetFile, size)
    print(metadata)
    close(parquetFile)
}

read.parquet("alltypes.plain.parquet")