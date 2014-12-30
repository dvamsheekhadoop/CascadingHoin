import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.h2.tools.DeleteDbFiles;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
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

	private static final String DBURL = "jdbc:mysql://localhost";
	private static final String DBNAME = "world";
	private static final String DBDRIVER = "com.mysql.jdbc.Driver";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "root";
	private static final String TABLE1 = "country";
	private static final String TABLE2 = "countrylanguage";
	private static final String[] COLUMNS1 = { "Code", "Name" };
	private static final String[] COLUMNS2 = { "CountryCode", "Language" };
	private static final String JOINCOLUMN1 = "Code";
	private static final String JOINCOLUMN2 = "CountryCode";
	private static final String HDFSOUTPUTPATH = "D:/cas/tt/";

	private static final String OUTPUTDELIMETER = "\t";

	public static void main(String[] args) throws ClassNotFoundException,
			Exception {

		// createH2Tables();

		JDBCScheme jdbcScheme1 = new JDBCScheme(COLUMNS1, null,
				new String[] { JOINCOLUMN1 });

		JDBCScheme jdbcScheme2 = new JDBCScheme(COLUMNS2, null,
				new String[] { JOINCOLUMN2 });

		Tap tapTable1 = new JDBCTap(DBURL + "/" + DBNAME, USERNAME, PASSWORD,
				DBDRIVER, TABLE1, jdbcScheme1);

		Tap tapTable2 = new JDBCTap(DBURL + "/" + DBNAME, USERNAME, PASSWORD,
				DBDRIVER, TABLE2, jdbcScheme2);

		Tap outTap = new Hfs(new TextDelimited(true, OUTPUTDELIMETER),
				HDFSOUTPUTPATH);

		Pipe firstTable = new Each("FirstTable", new Identity());

		Pipe secondTable = new Each("SecondTable", new Identity());

		Pipe joinPipe = new HashJoin(firstTable, new Fields(JOINCOLUMN1),
				secondTable, new Fields(JOINCOLUMN2), new InnerJoin());

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, Join.class);
		FlowConnector flowConnector = new HadoopFlowConnector(properties);

		FlowDef flowDef = FlowDef.flowDef().addSource(firstTable, tapTable1)
				.addSource(secondTable, tapTable2)
				.addTailSink(joinPipe, outTap);

		Flow readFlow = flowConnector.connect(flowDef);

		readFlow.complete();

	}

	private static void createH2Tables() throws ClassNotFoundException,
			SQLException {
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

	}
}
