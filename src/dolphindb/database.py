from dolphindb.table import Table
import uuid

def _generate_handle():
    return "TMP_TBL_" + uuid.uuid4().hex[:8]

class Database(object):
    def __init__(self, dbName=None, s=None):
        self.__dbName = dbName
        self.__session = s
    
    def _getDbName(self):
        return self.__dbName

    def createTable(self, table=None, tableName=None):
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")
        
        tHandle = table._getTableName()
        ctHandle = _generate_handle()
        runstr = ctHandle + "=" + self.__dbName + ".createTable(" + tHandle + "," + "`" + tableName + ");"
        self.__session.run(runstr)
        return self.__session.loadTable(ctHandle)

    def createPartitionedTable(self, table=None, tableName=None, partitionColumns=None):
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")
        
        tHandle = table._getTableName()
        cptHandle = _generate_handle()

        partitionColumns_str = ''
        if type(partitionColumns) == str:
            partitionColumns_str =  "`" + partitionColumns
        elif type(partitionColumns) == list:
            for col in partitionColumns:
                partitionColumns_str += '`'
                partitionColumns_str += col
        else:
            raise RuntimeError("Only String or List of String is accepted for partitionColumns")

        runstr = cptHandle + "=" + self.__dbName + ".createPartitionedTable(" + tHandle + "," + "`" + tableName + ","  + partitionColumns_str + ");"
        self.__session.run(runstr)
        return self.__session.loadTable(cptHandle)
