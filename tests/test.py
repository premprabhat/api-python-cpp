import dolphindb as ddb

def main():
    s = ddb.session()
    s.connect('localhost', 9921, 'admin', '123456')

    print(s.loadTable('t1', 'dfs://db').select('id').sort(bys=['id']).top(1).toDF())
    
main()
