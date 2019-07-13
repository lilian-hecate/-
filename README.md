# -
import xlrd,xlwt,pymysql  # 导入所需要的库
# 连接excel打开文件
filesname = ('basic inform','basic data')
Data = []
for i in range(len(filesname)):
    Data.append(xlrd.open_workbook('D:\\for python\\' + filesname[i] + '.xlsx'))
# 连接mysql
conn = pymysql.connect(
    host = 'localhost',
    user = 'root',
    passwd = '123456',
    port = 3308,
    charset = 'utf8'
)
cur = conn.cursor()   # 建游标 # 考虑自定义函数把执行-回滚-提交简化
# 清空需使用的数据库并进入
restar_sql1 = """drop database if exists test1"""
restar_sql2 = """create database test1"""
restar_sql3 = """use test1"""
R_sql = [restar_sql1,restar_sql2,restar_sql3]
try:
    for i in range(len(R_sql)):
        cur.execute(R_sql[i])
except Exception as e:
    conn.rollback()
    print('初始化失败',e)  # 此处可以考虑出错后直接结束程序
else:
    conn.commit()
    print('初始化成功',cur.rowcount)
# 创建表的sql
create_sql1 = """create table provincialorder(
                        id int unsigned primary key not null auto_increment,
                        province varchar(4) not null,
                        porder int unsigned not null,
                        d_null int
                        )engine=InnoDB auto_increment=1"""
create_sql2 = """create table levelweight(
                        id int unsigned primary key not null auto_increment,
                        allname varchar(30) not null,
                        alllevel varchar(8) not null,
                        weight float not null
                        )engine=InnoDB auto_increment=1"""
create_sql3 = """create table ewmprovince(
                        id int unsigned primary key not null auto_increment,
                        province varchar(4) not null,
                        ewm varchar(4) not null,
                        d_null int
                        )engine=InnoDB auto_increment=1"""
create_sql4 = """create table fourthpro(
                        id int unsigned primary key not null auto_increment,
                        tlevel varchar(8) not null,
                        prosymbol int not null,
                        d_null int
                        )engine=InnoDB auto_increment=1"""
create_sql5 = """create table allsuplevel(
                        id int unsigned primary key not null auto_increment,
                        alllevel varchar(30) not null,
                        suplevel varchar(8) not null,
                        d_null int
                        )engine=InnoDB auto_increment=1"""
create_sql6 = """create table fdata(
                        id int unsigned primary key not null auto_increment,
                        province varchar(4) not null,
                        year int unsigned not null,
                        fname varchar(30),
                        fvalue float
                        )engine=InnoDB auto_increment=1"""
C_sql = [create_sql1,create_sql2,create_sql3,create_sql4,create_sql5,create_sql6]
try:
    for i in range(len(C_sql)):
        cur.execute(C_sql[i])
except Exception as e:
    conn.rollback()
    print('表创建失败',e)
else:
    conn.commit()
    print('表创建成功',cur.rowcount)
# 读取信息表中数据、添加到数据库中表
wt_sql1 = """insert into provincialorder(porder,province,d_null)values(%s,%s,%s)"""
wt_sql2 = """insert into ewmprovince(province,ewm,d_null)values(%s,%s,%s)"""
wt_sql3 = """insert into allsuplevel(suplevel,alllevel,d_null)values(%s,%s,%s)"""
wt_sql4 = """insert into levelweight(allname,alllevel,weight)values(%s,%s,%s)"""
wt_sql5 = """insert into fourthpro(prosymbol,tlevel,d_null)values(%s,%s,%s)"""
W_sql = [wt_sql1,wt_sql2,wt_sql3,wt_sql4,wt_sql5]
Table1 = []
for i in range(len(Data[0].sheets())):
    Table1.append(Data[0].sheets()[i])
try:
    for i in range(len(Table1)):
        for j in range(Table1[i].nrows):
            if (Table1[i].cell(j,0).ctype) == 2:
                cur.execute(W_sql[i],(int(Table1[i].cell(j,0).value),Table1[i].cell(j,1).value,Table1[i].cell(j,2).value))
            else:
                cur.execute(W_sql[i],(Table1[i].cell(j,0).value,Table1[i].cell(j,1).value,Table1[i].cell(j,2).value))
except Exception as e:
    conn.rollback()
    print('信息数据导入失败',e)
else:
    conn.commit()
    print('信息数据导入成功',cur.rowcount)
# 读取数据表中的数据、添加到数据库中表
wt_sql = """insert into fdata(year,province,fname,fvalue)values(%s,%s,%s,%s)"""
Table2 = []
for i in range(len(Data[1].sheets())):
    Table2.append(Data[1].sheets()[i])
try:
    for i in range(len(Table2)):
        for k in range(1,Table2[i].ncols):
            for j in range(1,Table2[i].nrows):
                cur.execute(wt_sql,(Table2[i].cell(0,0).value,Table2[i].cell(j,0).value,Table2[i].cell(0,k).value,Table2[i].cell(j,k).value))
except Exception as e:
    conn.rollback()
    print('基础数据导入失败',e)
else:
    conn.commit()
    print('基础数据导入成功',cur.rowcount)
# 删去所有表中d_null列
Tablename = ['provincialorder','ewmprovince','fourthpro','allsuplevel']
D_sql = []
for i in range(len(Tablename)):
    D_sql.append('alter table ' + Tablename[i] + ' drop column d_null')
try:
    for i in range(len(D_sql)):
        cur.execute(D_sql[i])
except Exception as e:
    conn.rollback()
    print('重复列删除失败',e)
else:
    conn.commit()
    print('重复列删除成功',cur.rowcount)
# 备份data表，根据表关系加上t_name，删去f_name，group by同时sum值
pro4_sql1 = """create table profdata select * from fdata"""
pro4_sql2 = """alter table profdata add tlevel varchar(8) not null"""
pro4_sql3 = """update profdata inner join allsuplevel on profdata.fname = allsuplevel.alllevel set profdata.tlevel = allsuplevel.suplevel;"""
pro4_sql4 = """alter table profdata drop column fname"""
pro4_sql5 = """create table tdata as (select year,province,sum(fvalue),tlevel from profdata group by year,province,tlevel)"""
P4_sql = [pro4_sql1,pro4_sql2,pro4_sql3,pro4_sql4,pro4_sql5]
try:
    for i in range(len(P4_sql)):
        cur.execute(P4_sql[i])
except Exception as e:
    conn.rollback()
    print('4->3失败',e)
else:
    conn.commit()
    print('4->3成功',cur.rowcount)
# 需要的除以[人口]，取得最大值并全体除以，乘以权重

#sql1 = """create table t_data as (select year,province,basic_f_inform.f_name,f_value as t_value,sup_level as t_name from basic_f_inform,fourth_inform where basic_f_inform.f_name = fourth_inform.f_name)"""
cur.close()
conn.close()
