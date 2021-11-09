package ink.haifeng.test;

import cn.hutool.core.text.StrFormatter;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.ds.simple.SimpleDataSource;
import cn.hutool.db.handler.RsHandler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GenerateVisit {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:mysql://192.168.31.88:3306/db_flink";
        SimpleDataSource dataSource = new SimpleDataSource(url, "root", "haifeng123");
        Db db = Db.use(dataSource);
        String sql = "select device from tb_region_device";
        List<String> devices = db.query(sql, String.class);
        long timestamp = System.currentTimeMillis();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 3600; i++) {
            timestamp = timestamp + RandomUtil.randomInt(1, 30) * 1000L;
            String uid = StrUtil.padPre(String.valueOf(RandomUtil.randomInt(1, 100)), 3, "u");
            sql = StrFormatter.format("insert into tb_visit_log (uid,device,visit_time) values('{}','{}',{})",
                    uid, devices.get(RandomUtil.randomInt(0, devices.size())), timestamp / 1000);
            list.add(sql);
        }
        db.executeBatch(list);
        System.out.println(1);
    }
}
