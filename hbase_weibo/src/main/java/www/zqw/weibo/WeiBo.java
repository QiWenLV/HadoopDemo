package www.zqw.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * 发布微博
 * 互粉
 * 取关
 * 查看微博
 */
public class WeiBo {

    //创建weibo这个业务的命名空间，和三张表

    //命名空间名称
    public static final byte[] NS_WERBO = Bytes.toBytes("ns_weibo");
    //微博内容表
    public static final byte[] TABLE_CONTENT = Bytes.toBytes("ns_weibo:content");

    //用户关系表
    public static final byte[] TABLE_RELATION = Bytes.toBytes("ns_weibo:relation");

    //收件箱表
    public static final byte[] TABLE_INBOX = Bytes.toBytes("ns_weibo:inbox");

    //HBase的配置对象 Configuration
    private Configuration conf = HBaseConfiguration.create();

    //初始化
    private void init() throws IOException {
        //初始化命名空间
        initNamespace();
        //初始化表
        initTableContent();
        initTableRealation();
        initTableInbox();

    }

    //初始化命名空间
    private void initNamespace() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //命名空间描述器
        NamespaceDescriptor ns_weibo = NamespaceDescriptor
                .create("ns_weibo")
                .addConfiguration("creator", "zqw")
                .addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
                .build();
        admin.createNamespace(ns_weibo);
        //关闭资源
        admin.close();
        connection.close();
    }


    /**
     * 创建微博内容表
     *
     * 表名：ns_weibo:content
     * 列族名：info
     * 列名：content
     * rowkey：用户id_时间戳
     * value：微博内容
     * version：1
     * @throws IOException
     */
    private void initTableContent() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
        //创建列族描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");

        //设置块缓存
        infoColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        infoColumnDescriptor.setBlocksize(2 * 1024 * 1024); //缓存2M 上传一次
        //设置版本个数
        infoColumnDescriptor.setMinVersions(1);
        infoColumnDescriptor.setMaxVersions(1);

        //将列族描述器添加到表描述器中
        contentTableDescriptor.addFamily(infoColumnDescriptor);
        //创建表
        admin.createTable(contentTableDescriptor);

        admin.close();
        connection.close();

    }

    /**
     * 创建用户关系表
     *
     * 表名：ns_weibo:relation
     * 列族名：attends, fans
     * 列名：用户id
     * rowkey：当前操作人的用户id
     * value：用户id
     * version：1
     * @throws IOException
     */
    private void initTableRealation() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //表描述器
        HTableDescriptor relationDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
        //列族描述器(两个列族)
        HColumnDescriptor attendsColumnDescriptor = new HColumnDescriptor("attends");
        HColumnDescriptor fansColumnDescriptor = new HColumnDescriptor("fans");

        //设置缓存块
        attendsColumnDescriptor.setBlockCacheEnabled(true);
        attendsColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        fansColumnDescriptor.setBlockCacheEnabled(true);
        fansColumnDescriptor.setBlocksize(2 * 1024 * 1024);

        //设置版本个数
        attendsColumnDescriptor.setMinVersions(1);
        attendsColumnDescriptor.setMaxVersions(1);
        fansColumnDescriptor.setMinVersions(1);
        fansColumnDescriptor.setMaxVersions(1);

        //创建表
        relationDescriptor.addFamily(attendsColumnDescriptor);
        relationDescriptor.addFamily(fansColumnDescriptor);
        admin.createTable(relationDescriptor);

        admin.close();
        connection.close();
    }

    /**
     * 创建收件箱表
     *
     * 表名：ns_weibo:inbox
     * 列族名：info
     * 列名：当前用户所关注的人的用户id
     * rowkey：当前操作人的用户id
     * value：消息的rowkey
     * version：50
     * @throws IOException
     */
    private void initTableInbox() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //表描述器
        HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
        //列族描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");

        //设置缓存块
        infoColumnDescriptor.setBlockCacheEnabled(true);
        infoColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        //设置版本个数
        infoColumnDescriptor.setMinVersions(100);
        infoColumnDescriptor.setMaxVersions(100);

        //创建表
        inboxTableDescriptor.addFamily(infoColumnDescriptor);
        admin.createTable(inboxTableDescriptor);

        admin.close();
        connection.close();
    }


    /**
     * 发布微博
     * 1. 向微博表中添加刚发布的内容，
     * 2. 向微博发布人的粉丝的收件箱表中，添加该条微博
     * @param uid   用户Id
     * @param content   微博内容
     */
    public void publishContent(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        //获得微博表对象
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));


        //组装微博rowkey
        long ts = System.currentTimeMillis();
        String rowkey = uid + "_" + ts;
        //向微博表添加数据
        Put contentPut = new Put(Bytes.toBytes(rowkey));
        contentPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"),  Bytes.toBytes(content));
        contentTable.put(contentPut);

        //在联系人表中查询用户的粉丝id
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        //设置筛选条件
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        //获取粉丝的ID
        Result result = relationTable.get(get);

        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));


        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            //粉丝Id
            byte[] fansId = CellUtil.cloneQualifier(cell);


            //向粉丝收件箱发送微博
            Put putInbox = new Put(fansId);
            putInbox.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), ts, Bytes.toBytes(rowkey)); //手动指定版本的时间戳
            inboxTable.put(contentPut);
        }

        inboxTable.close();
        relationTable.close();
        contentTable.close();
        connection.close();
    }


    /**
     * 用户添加关注
     * 1. 在用户关系表中，对为用户Id进行添加关注的操作
     * 2. 在用户关系表中，对被关注的人的用户id，添加粉丝操作
     * 3. 对当前操作的用户的收件箱中，添加他所关注的人的最近微博rowkey
     * @param uid
     * @param attends
     */
    public void addAttends(String uid, String... attends) throws IOException {
        //过滤参数
        if(attends == null || attends.length <=0 || uid == null) return;

        //获取连接，创建表对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        //为关注列族添加数据
        Put attendPut = new Put(Bytes.toBytes(uid));

        List<Put> puts = new ArrayList<>();
        for (String attend : attends) {
            //1. 为用户row添加粉丝
            attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));

            //2. 为被关注人的row中添加粉丝
            Put fansPut = new Put(Bytes.toBytes(attend));
            fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);

        }
        puts.add(attendPut);
        //优化，一起添加多条类型不同的put
        relationTable.put(puts);

        //3. 在消息表中查出被关注人的微博
        Scan scan = new Scan();
        //用于存放扫描出来的，我新关注的人的最近微博rowkey
        List<byte[]> rowkeys = new ArrayList<>();

        for (String attend : attends) {
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(filter);
            //通过scan拿到扫描结果
            ResultScanner resultScanner = contentTable.getScanner(scan);

            Iterator<Result> iterator = resultScanner.iterator();
            while (iterator.hasNext()){
                Result result = iterator.next();
                //获得微博rowkey
                byte[] row = result.getRow();
                rowkeys.add(row);
            }
        }
        //将获取的rowkey放在用户的收件箱中
        if(rowkeys.size() <= 0) return;

        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (byte[] rowkey : rowkeys) {
            //获取消息所属用户的id
            String[] attendIdAndWeiboTS = Bytes.toString(rowkey).split("_");
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendIdAndWeiboTS[0]), Long.parseLong(attendIdAndWeiboTS[1]), rowkey);
        }
        inboxTable.put(inboxPut);

        inboxTable.close();
        contentTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 取关操作
     * 1. 在用户关系表中，删除要取关的人的用户ID
     * 2. 在用户关系表中，删除被你取关的人的粉丝中的当前操作人的ID
     * 3. 删除用户收件箱中被取关人的消息
     * @param uid
     * @param attends
     */
    public void removeAttends(String uid, String... attends) throws IOException {
        //过滤参数
        if(attends == null || attends.length <=0 || uid == null) return;
        //获取连接，创建表对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

        //保存被取关人中要删除的用户Delete
        List<Delete> deleteList = new ArrayList<>();

        Delete attendDelete = new Delete(Bytes.toBytes(uid));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for (String attend : attends) {
            //在用户关注列族中删除取关的人
            attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
            //在被取关的人的粉丝列族中删除该用户
            Delete fansDelete = new Delete(Bytes.toBytes(attend));
            fansDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deleteList.add(fansDelete);

            //删除当前用户收件箱中被取关者的信息
//

        }
        deleteList.add(attendDelete);

        relationTable.delete(deleteList);

        for (String attend : attends) {
            inboxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(attend));
        }
        inboxTable.delete(inboxDelete);

        //关闭资源
        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 查看微博内容
     * 1. 从微博收件箱中获取所有关注的人发布的微博的微博rowkey
     * 2. 根据得到的微博rowkey，在内容表中查出微博内容
     * 3. 将取出来的数据节码，放在Message对象中
     * @param uid
     * @return
     */
    public List<Message> getAttendsContent(String uid) throws IOException {

//        if(uid == null) return
        Connection connection = ConnectionFactory.createConnection(conf);
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Table contextTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        //筛选条件
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.addFamily(Bytes.toBytes("info"));
        //每个Cell中存储了100个版本，需要获取5个版本
        inboxGet.setMaxVersions(5);
        Result result = inboxTable.get(inboxGet);

        //准备一个存放所有微博rowkey的集合
        List<byte[]> rowkeys = new ArrayList<>();

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            //获取到微博rowkey
            rowkeys.add(CellUtil.cloneValue(cell));
        }

        //2.获取微博内容
        List<Get> contextGets = new ArrayList<>();
        for (byte[] rowkey : rowkeys) {
            Get contextGet = new Get(rowkey);
            contextGet.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"));
            contextGets.add(contextGet);
        }
        Result[] contextResults = contextTable.get(contextGets);

        //存放结果的List
        List<Message> messageList = new ArrayList<>();

        for (Result r : contextResults) {
            Cell[] contextCells = r.rawCells();
            String[] key_time = Bytes.toString(r.getRow()).split("_");
            for (Cell c : contextCells) {
                //构建消息对象
                Message msg = new Message();
                msg.setUid(key_time[0]);
                msg.setTimestamp(Long.valueOf(key_time[1]));
                msg.setContent(Bytes.toString(CellUtil.cloneValue(c)));

                messageList.add(msg);
            }
        }

        contextTable.close();
        inboxTable.close();
        connection.close();

        return messageList;
    }

    /**
     * 查询用户的关注列表
     * @param uid
     */
    public void allAttends(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table relationtable = connection.getTable(TableName.valueOf(TABLE_RELATION));

        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("attends"));

        Result result = relationtable.get(get);
        List<byte[]> attendList = new ArrayList<>();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            attendList.add(CellUtil.cloneValue(cell));
        }

        for (byte[] attend : attendList) {
            System.out.println(Bytes.toString(attend));
        }

    }


    /**
     * 测试用例
     */
    //发布微博
    public static void publishWeiBoTest(WeiBo weiBo, String uid, String content) throws IOException{
        weiBo.publishContent(uid, content);
    }

    //关注
    public static void addAttendTest(WeiBo weiBo, String uid, String... attends) throws IOException{
        weiBo.addAttends(uid, attends);
    }

    //取关
    public static void removeAttendTest(WeiBo weiBo, String uid, String... attends) throws IOException{
        weiBo.removeAttends(uid, attends);
    }

    //刷微博
    public static void scanContentTest(WeiBo weiBo, String uid) throws IOException{
        List<Message> list = weiBo.getAttendsContent(uid);
        for (Message message : list) {
            System.out.println(message);
        }
    }

    //关注
    //取关
    //刷微博
    public static void main(String[] args) throws IOException {
        WeiBo wb = new WeiBo();

//        wb.init();

        //发微博
//        publishWeiBoTest(wb, "1002", "哦哦哦，这是一条微博1");
//        publishWeiBoTest(wb, "1002", "哦哦哦，这是一条微博2");
//        publishWeiBoTest(wb, "1002", "哦哦哦，这是一条微博3");
//        publishWeiBoTest(wb, "1002", "哦哦哦，这是一条微博4");
//        publishWeiBoTest(wb, "1003", "呀呀呀，2233");
//        publishWeiBoTest(wb, "1003", "呀呀呀呀，2233");

        //添加关注
//        addAttendTest(wb, "1001", "1002", "1003");
//

//        addAttendTest(wb, "1003", "1002");
        removeAttendTest(wb, "1003", "1002");
        scanContentTest(wb, "1003");

        wb.allAttends("1003");

    }
}
