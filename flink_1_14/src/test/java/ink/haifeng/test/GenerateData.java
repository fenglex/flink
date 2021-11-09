package ink.haifeng.test;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.StrFormatter;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.ds.simple.SimpleDataSource;

import java.sql.SQLException;

public class GenerateData {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:mysql://192.168.31.88:3306/db_flink";
        SimpleDataSource dataSource = new SimpleDataSource(url, "root", "haifeng123");
        Db db = Db.use(dataSource);
        int did = 0;
        int id = 0;
        String sql = "insert into tb_region_device (id,region,device,create_time,update_time) values ({},'{}','{}','{}','{}')";
        long timestamp = System.currentTimeMillis();
        for (int i = 1; i < 10; i++) {
            int random = RandomUtil.randomInt(1, 6);
            for (int j = 1; j <= random; j++) {
                did = did + 1;
                timestamp = timestamp + RandomUtil.randomInt(1, 10) * 1000L;
                String format = DateUtil.format(DateUtil.date(timestamp), "yyyy-MM-dd HH:mm:ss");
                String device = StrUtil.padPre(String.valueOf(did), 3, "D");
                String execSql = StrFormatter.format(sql, id + j, "R0" + i, device, format, format);
                db.execute(execSql);
                System.out.println(execSql);
            }
            id = id + random;
        }


    }
}
