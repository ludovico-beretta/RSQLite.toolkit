dbReadFileTable <- function(input.file, schema.file, header=FALSE,
                            dbcon, table.name, drop.table=FALSE,
                            auto.pk=FALSE, build.pk=FALSE,
                            chunk.size=10000, constant.values=NULL, ...) {

    ## formal checks on parameters ................
    if (!is.list(constant.values) && !is.null(constant.values)) {
        stop("dbReadFileTable: error in 'constant.values' parameters: must be a list.")
    }

    if (is.list(constant.values) && length(constant.values)==0) {
        stop("dbReadFileTable: 'constant.values' list must not be of zero length.")
    }


    ## read schema ................................
    df.scm <- read.table(file=schema.file, header=TRUE, sep=",",
                         skip=0,quote="",comment.char="", row.names=NULL,
                         colClasses=rep("character",times=4),
                         col.names=c("VARNAME","SQLTYPE","RTYPE","PK"),
                         strip.white=TRUE,stringsAsFactors=FALSE)


    ## create empty table .........................
    autoPK <- FALSE
    if (length(df.scm[which(df.scm$PK=="Y"),"VARNAME"])==0 && auto.pk) autoPK <- TRUE

    if (drop.table) {

        sql.def <- paste("DROP TABLE IF EXISTS ", table.name, ";", sep="")
        dbExecute(dbcon, sql.def)
    }


    sql.head <- paste("CREATE TABLE IF NOT EXISTS ", table.name, " (", sep="")
    sql.body <- paste(df.scm$VARNAME, df.scm$SQLTYPE, sep=" ", collapse=", ")

    if (!is.null(constant.values)) {

        for (ii in 1:length(constant.values)) {
            if (is.null(names(constant.values)[ii])) {
                fld.name <- paste("V",ii,sep="")
            } else {
                fld.name <- names(constant.values)[ii]
            }

            if (data.class(constant.values[[ii]])=="character") {
                fld.type <- "TEXT"
            } else if (data.class(constant.values[[ii]])=="numeric") {
                fld.type <- "REAL"
            } else if (data.class(constant.values[[ii]])=="Date") {
                fld.type <- "DATE"
            } else if (data.class(constant.values[[ii]])=="integer") {
                fld.type <- "INTEGER"
            } else {
                fld.type <- "TEXT"
            }

            sql.body <- paste(sql.body, ", ", fld.name, " ", fld.type, sep="")
        }
    }

    if (autoPK) {
        sql.body <- paste(sql.body, ", SEQ INTEGER PRIMARY KEY", sep="")
    }

    sql.tail <- ");"
    sql.def <- paste(sql.head, sql.body, sql.tail, sep=" ")

    dbExecute(dbcon, sql.def)




    ## columns types for reading data .............
    cnames <- df.scm$VARNAME
    cclass <- df.scm$RTYPE

    lclass <- list()
    for (ii in 1:length(cclass)) {
        if (cclass[ii] == "Date") {
            lclass[[ii]] <- vector("character", 0)
        } else {
            lclass[[ii]] <- vector(cclass[ii], 0)
        }
    }

    ## read data ..................................
    fcon <- file(input.file, "r", blocking = FALSE)

    if (header) {
        scan(file=fcon, what=character(), nlines=1)
    }

    nread <- 0
    repeat {

        dfbuffer <- scan(file=fcon,
                         what=lclass,
                         nlines=chunk.size,
                         strip.white=TRUE,flush=TRUE,fill=TRUE,
                         multi.line=FALSE,quiet=TRUE, ...)

        if (length(dfbuffer[[1]])==0) break

        dfbuffer <- as.data.frame(dfbuffer,row.names=NULL, stringAsFactors=FALSE)

        if (!is.null(constant.values)) {
            dfbuffer <- cbind(dfbuffer, constant.values)
        }

        names(dfbuffer) <- cnames

        dfbuffer[,which(cclass=="Date")] <- format(dfbuffer[,which(cclass=="Date")],
                                                   format="%Y-%m-%d")

        if (autoPK) {
            dfbuffer <- cbind(dfbuffer,NA)
        }

        dbWriteTable(dbcon, table.name, dfbuffer, row.names=FALSE, append=TRUE)
        nread <- nread+chunk.size

    }

    close(fcon)

    ## Indexing -------------------------------
    if (drop.table && length(df.scm[which(df.scm$PK=="Y"),"VARNAME"])>0 && build.pk) {
        cnames <- df.scm[which(df.scm$PK=="Y"),"VARNAME"]
        dbExecute(dbcon, paste(
            "CREATE UNIQUE INDEX ", paste(table.name,"_PK",sep=""),
            "ON ", table.name," (", paste(cnames, collapse=", "),
            ");", sep=" ")
        )
    }
}
