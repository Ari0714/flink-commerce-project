import com.atguigu.gmall.realtime.utils.PhoenixUtil;

import java.sql.SQLException;

public class Test {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

//        PhoenixUtil.init();

        System.out.println(PhoenixUtil.queryList("select * from user_status2020 where user_id = '1001'", String.class));

//        System.out.println(PhoenixUtil.queryOne("select * from user_status2020 where user_id = '1001'", PhoenixUtil.getConn()));

    }
}
