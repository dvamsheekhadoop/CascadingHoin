import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.h2.tools.DeleteDbFiles;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Join {

	public static void main(String[] args) throws ClassNotFoundException,
			Exception {
		DeleteDbFiles.execute("~", "test", true);
		DeleteDbFiles.execute("~", "test2", true);
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
		Statement stat = conn.createStatement();

		// this line would initialize the database
		// from the SQL script file 'init.sql'
		// stat.execute("runscript from 'init.sql'");

		stat.execute("create table test(id int primary key, name varchar(255))");
		stat.execute("insert into test values(1, 'Hello')");
		stat.execute("insert into test values(2, 'Hello2')");
		stat.execute("insert into test values(3, 'Hello3')");

		stat.execute("create table test2(tid int primary key, loc varchar(255))");
		stat.execute("insert into test2 values(1, 'Hello1Loc')");
		stat.execute("insert into test2 values(2, 'Hello2Loc')");
		stat.execute("insert into test2 values(3, 'Hello3Loc')");

		ResultSet rs;
		rs = stat.executeQuery("select * from test");
		while (rs.next()) {
			System.out.println(rs.getString("name"));
		}

		ResultSet rs1;
		rs1 = stat.executeQuery("select * from test2");
		while (rs1.next()) {
			System.out.println(rs1.getString("loc"));
		}
		stat.close();
		conn.close();

		String[] columnNames1 = { "id", "name" };

		String[] columnNames2 = { "tid", "loc" };

		JDBCScheme jdbcScheme1 = new JDBCScheme(columnNames1, null,
				new String[] { "id" });

		JDBCScheme jdbcScheme2 = new JDBCScheme(columnNames2, null,
				new String[] { "tid" });

		Tap h2tap1 = new JDBCTap("jdbc:h2:~/test", "", "", "org.h2.Driver",
				"test", jdbcScheme1);

		// Change it to mysql db
		Tap h2tap2 = new JDBCTap("jdbc:h2:~/test2", "", "", "org.h2.Driver",
				"test", jdbcScheme2);

		Tap outTap = new Hfs(new TextDelimited(true, "\t"), "D:/cas/tt/");

		Pipe pipe1 = new Each("pipe1", new Identity());

		Pipe pipe2 = new Each("pipe2", new Identity());

		Pipe joinPipe = new HashJoin(pipe1, new Fields("id"), pipe2,
				new Fields("name"), new InnerJoin());

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, Join.class);
		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

		FlowDef flowDef = FlowDef.flowDef().addSource(pipe1, h2tap1)
				.addSource(pipe2, h2tap2).addTailSink(joinPipe, outTap);

		Flow readFlow = flowConnector.connect(flowDef);

		readFlow.complete();

	}
}
