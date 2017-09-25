package com.github.rtempleton.poncho;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Test;

import com.github.rtempleton.poncho.test.AbstractTestCase;

public class TestPoncho extends AbstractTestCase {
	
	private final String propsFile = getResourcePath() + "/config.properties";
	private final String sampleFile = getResourcePath() + "/io/TestFile.txt";
	
	
	@Ignore
	public void testKafkaLoader(){
		TextFileProducer producer = new TextFileProducer(propsFile, sampleFile);
		producer.run();
	}
	
	
	/*
	 * Utility test I used to gather metadata as a result of a select statement. Used to create the destination table where these results would eventually be written
	 * Tyson POC
	 */
	@Test
	public void testGetPhoenixMetadata() throws Exception{
		
		final String jdbcurl = "jdbc:phoenix:rtempleton0.field.hortonworks.com:2181:/hbase-unsecure";
		final String query = "select ekpo.EBELN as PO_DOC, \n" + 
				"                ekpo.EBELP as PO_DOC_ITEM_NUM,\n" + 
				"                ekbe.BELNR as DELV_DOC,\n" + 
				"                ekpo.LOEKZ as PO_LINE_DELETE_IND,\n" + 
				"                ekpo.AEDAT as PO_LINE_MOD_DATE,\n" + 
				"                ekpo.MATNR as MTRL,\n" + 
				"                ekpo.WERKS as RECV_PLANT,\n" + 
				"                ekpo.LGORT as STO_LINE_STOR_LOC,\n" + 
				"                ekpo.MENGE as PO_LINE_QTY,\n" + 
				"                ekpo.MEINS as PO_LINE_QTY_UOM,\n" + 
				"                ekpo.BPRME as PO_LINE_NET_PRICE_AMT_UOM,\n" + 
				"                ekpo.NETPR as PO_LINE_NET_PRICE_AMT,\n" + 
				"                ekpo.ELIKZ as PO_LINE_COMPLETE_IND,\n" + 
				"                ekpo.PSTYP as PO_LINE_CAT,\n" + 
				"                ekpo.EVERS as DELV_DOC_SHIP_INSTRUCTIONS,\n" + 
				"                t027b.EVTXT as STO_SHIP_INSTRN,\n" + 
				"                ekpo.BSTAE as CONFIRMATION_CONTROL_KEY,\n" + 
				"                t163m.BSBEZ as STO_SHIP_DESCR,\n" + 
				"                ekpo.EGLKZ as STO_LINE_FINAL_DELV_IND,\n" + 
				"                ekpo.KZTLF as STO_LINE_PART_DELV_IND,\n" + 
				"                eket.ETENR as PO_LINE_SCHED_LINE_ITEM,\n" + 
				"                eket.EINDT as PO_LINE_SCHED_DELV_DATE,\n" + 
				"                eket.MENGE as PO_LINE_SCHED_QTY,\n" + 
				"                eket.WEMNG as PO_LINE_SCHED_DELV_QTY,\n" + 
				"                eket.UZEIT as PO_LINE_SCHED_DELV_TIME,\n" + 
				"                ekpv.LPRIO as STO_LINE_DELV_PRIORITY,\n" + 
				"                ekpv.VSBED as STO_LINE_SHIP_CONDITION,\n" + 
				"                ekko.BSART as PO_TYPE,\n" + 
				"                ekko.LOEKZ as PO_DELETE_IND,\n" + 
				"                ekko.AEDAT as PO_CREATE_DATE,\n" + 
				"                ekko.ERNAM as PO_CREATE_USERID,\n" + 
				"                ekko.LIFNR as VEND_NAME1,\n" + 
				"                ekko.WAERS as PO_CRN,\n" + 
				"                ekko.BEDAT as PO_DOC_DATE,\n" + 
				"                ekko.RESWK as SHIP_PLANT,\n" + 
				"                case when ekpo.MENGE = 0 then 0 else ekpo.NETPR/ekpo.MENGE end as NET_PRICE_UNIT_AMT,\n" + 
				"                round(ekpo.MENGE * (ekpo.NETPR/ekpo.MENGE)) as STO_LINE_SCHED_LINE_NET_PRICE_AMT\n" + 
				"from eket\n" + 
				"join ekpo on (eket.EBELN = ekpo.EBELN and eket.EBELP = ekpo.EBELP)\n" + 
				"join ekko on (ekko.EBELN = ekpo.EBELN and ekko.BSART in ('UB', 'STO'))\n" + 
				"left outer join ekpv on (ekpv.EBELN = ekpo.EBELN and ekpv.EBELP = ekpo.EBELP)\n" + 
				"left outer join t163m on (ekpo.BSTAE = t163m.BSTAE and t163m.SPRAS = 'E')\n" + 
				"left outer join t027b on (ekpo.EVERS = t027b.EVERS and t027b.SPRAS = 'E')\n" + 
				"left outer join ekbe on (ekpo.EBELN = ekbe.EBELN and ekpo.EBELP = ekbe.EBELP)\n" + 
				"where ekbe.BEWTP = 'L'\n" + 
				"and ekbe.MENGE != 0\n" + 
				"and ekbe.VGABE = '8'\n" + 
				"and ekpo.EBELN = ?";
		
		Connection con = DriverManager.getConnection(jdbcurl);
		con.setAutoCommit(false);
		PreparedStatement stmt = con.prepareStatement(query);
		stmt.setString(1, "TEST");
//		System.out.println(stmt.execute());
		ResultSet rset = stmt.executeQuery();
		ResultSetMetaData meta = rset.getMetaData();
		
		for (int i=1; i<meta.getColumnCount()+1; i++) {
			System.out.println(meta.getColumnName(i) + " " + meta.getColumnTypeName(i));
		}
		
		rset.close();
		stmt.close();
		con.close();
		
	}

}
