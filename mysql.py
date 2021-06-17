
from config import *
import pymysql

class MySQL():
    def __init__(self, host=MYSQL_HOST, username=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE):
        """
        MySQL 初始化
        :param host:
        :param username:
        :param password:
        :param port:
        :param database:
        """
        try:
            self.db = pymysql.connect(host=host, user=username, password=password, database=database, charset='utf8')
            self.cursor = self.db.cursor()
        except pymysql.MySQLError as e:
            print(e.args)
    
    #预售链接存入mysql并去重
    def insert_ItemID(self, table, data):
        """
        插入数据
        :param table:
        :param data:
        :return:
        """
        
        head=['id','item_name','item_link','store_name']
        keys = ','.join(head)
        values = ','.join(['%s'] * len(head))
        
        #去重
        sql_dup = 'SELECT id FROM %s WHERE id=%s' % (table, data[0])
        res = self.cursor.execute(sql_dup)
        if res>0:  # res为查询到的数据条数如果大于0就代表数据已经存在
            return 0
        
        #新增链接
        sql_query = 'insert into %s (%s) values(%s)' % (table, keys, values)
        try:
            self.cursor.execute(sql_query, data)
            self.db.commit()
            return 1
        except pymysql.MySQLError as e:
            print(e.args)
            self.db.rollback()
            
    #数据库获取全部预售链接
    def fetch_Item(self):
        sql_items = "SELECT id,item_link FROM ItemID"
        self.cursor.execute(sql_items)
        items = self.cursor.fetchall()
        id_list = []
        item_links = []
        for i in items:
            id_list.append(i[0])
            item_links.append(i[1])
            
        item_dict = dict(zip(id_list,item_links))
        return item_dict
    
    
    def create_ItemTable(self):
        #建表
        
        #sql_drop = "DROP TABLE IF EXISTS ItemID;"
        #self.cursor.execute(sql_drop)
        
        sql = """
            CREATE TABLE IF NOT EXISTS ItemID(
            id varchar(20) NOT NULL PRIMARY KEY,
            item_link varchar(255) NOT NULL,
            item_name varchar(255) NOT NULL,
            store_name varchar(255) NOT NULL
            ) DEFAULT CHARSET=utf8;
        """
        self.cursor.execute(sql)
        
        
    def create_yushouTable(self,table):
        #创建新表,预售链接的相关信息
        sql = """
            CREATE TABLE IF NOT EXISTS %s(
            id varchar(20) NOT NULL PRIMARY KEY,
            crawl_time DATETIME NOT NULL,
            yushou_order varchar(255) NOT NULL,
            yushou_price varchar(255) NOT NULL
                );
        """ % (table)
        
        self.cursor.execute(sql)
        
    
    def insert_yushou(self,table,data):
        #增加 or 更新
        sql_dup = 'SELECT id FROM %s WHERE id=%s' % (table, data[0])
        res = self.cursor.execute(sql_dup)
        if res>0:  # res为查询到的数据条数如果大于0就代表数据已经存在
            sql_update = """
                UPDATE %s
                SET crawl_time=%s, yushou_order=%s,
                WHERE id = %s
            """%(table,data[1],data[2],data[0])
            try:
                self.cursor.execute(sql_update)
                self.db.commit()
                return 0
            except pymysql.MySQLError as e:
                print(e.args)
                self.db.rollback()
        else:
            #增加预售链接的相关信息
            head=['id','crawl_time','yushou_order','yushou_price']
            keys = ','.join(head)
            values = ','.join(['%s'] * len(head))
            sql_add = 'insert into %s (%s) values(%s)' % (table, keys, values)
            try:
                self.cursor.execute(sql_add, data)
                self.db.commit()
                return 1
            except pymysql.MySQLError as e:
                print(e.args)
                self.db.rollback()
        
        
    #关闭mysql
    def db_close(self):
        self.cursor.close()
        self.db.close()

