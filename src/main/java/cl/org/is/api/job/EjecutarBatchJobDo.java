package cl.org.is.api.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


/**
 * 
 *  @description Clase que permite identificar ctl que no estan en roble desde WMS hacias JDA
 *
 */
public class EjecutarBatchJobDo {

	private static BufferedWriter bw;
	private static String path;
	
	private static final int DIFF_HOY_FECHA_INI = 1;
	private static final int DIFF_HOY_FECHA_FIN = 1;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
	}

	private static void crearTxt(Map <String, String> mapArguments) {

		Connection dbconnection = crearConexion();
		Connection dbconnOracle = crearConexionOracle();
		Connection dbconnOracleKpi = crearConexionOracleKpi();
		
		File file1              = null;
		BufferedWriter bw       = null;
		BufferedWriter bw2      = null;
		PreparedStatement pstmt = null;
		StringBuffer sb         = null;
		String iFechaIni           = null;
		String iFechaFin           = null;
		
		String iFechaIni2           = null;
		String iFechaFin2           = null;
		
		String iFechaIni3           = null;
		String iFechaFin3           = null;
		
		String iFechaIni4           = null;
		String iFechaFin4           = null;
		
		PreparedStatement pstmt2 							= null;

		try {

			try {
				
				iFechaIni = restarDias(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin = restarDias(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				iFechaIni2 = restarDias2(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin2 = restarDias2(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				iFechaIni3 = restarDiasTxt(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin3 = restarDiasTxt(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				iFechaIni4 = restarDiasDel(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin4 = restarDiasDel(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				info("[iFechaIni]"+iFechaIni);
				info("[iFechaFin]"+iFechaFin);
				info("[iFechaIni2]"+iFechaIni2);
				info("[iFechaFin2]"+iFechaFin2);
				info("[iFechaIni3]"+iFechaIni3);
				info("[iFechaFin3]"+iFechaFin3);
				info("[iFechaIni4]"+iFechaIni4);
				info("[iFechaFin4]"+iFechaFin4);
			}
			catch (Exception e) {

				e.printStackTrace();
			}
			file1           = new File(path + "/CuadraturaCtl-"  + iFechaIni3 + "_"+ iFechaFin3 + ".txt");
			
			
			Thread.sleep(60);
			info("Pausa para Eliminar cuadratura_do sleep(60 seg)");
			eliminar(dbconnOracleKpi,"DELETE FROM  cuadratura_do  where 1 = 1 AND F_CREACION >= '"+iFechaIni4+" 00:00:00' AND F_CREACION <= '"+iFechaFin4+" 23:59:59'");
			//eliminar(dbconnOracleKpi,"DELETE FROM  cuadratura_do  where 1 = 1");
			commit(dbconnOracleKpi,"COMMIT");
			
			Thread.sleep(60);
			info("Pausa para insertar cuadratura_ctl sleep(60 seg)");
			String sql = "Insert into cuadratura_do (ORDENNUM,EST_SC,F_CREACION,ETA,FECHALIBERADA,FACILITY,SOLCLIENTE,DO_EOM,EST_DO,T_ORDEN,ANT_EST,NUEV_EST,DO_WMS,SC,ESTADO) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			pstmt2 = dbconnOracleKpi.prepareStatement(sql);
			
			
			
			sb = new StringBuffer();
			//sb.append("select a.LOAD_NBR, a.WHSE, REPLACE(TO_CHAR(to_date(to_char(a.mod_date_time,'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY/MM/DD HH24:MI:SS'),'/','-') as mod_date_time, REPLACE(TO_CHAR(to_date(to_char(a.mod_date_time,'YYYYMMDD'), 'YYYYMMDD'), 'YYYYMMDD'),'/','-') as mod_date_time2, (select count(*) from gdd_hdr g where a.load_nbr = g.load_nbr) cant_guias  from OUTBD_LOAD a, outpt_OUTBD_LOAD b where a.load_nbr=b.load_nbr  and a.stat_code in ('80','90') and a.mod_date_time BETWEEN           TO_DATE('"+iFechaIni+" 00:00:00', 'DD-MM-YYYY HH24:MI:SS') AND  TO_DATE('"+iFechaFin+" 23:59:59', 'DD-MM-YYYY HH24:MI:SS') ");
			sb.append(" SELECT ca14.PO.purchase_orders_id        as OrdenNum ");
			sb.append(", ca14.PO.PURCHASE_ORDERS_STATUS    AS Est_SC ");
			//sb.append(", ca14.PO.created_dttm              as F_Creacion ");
			sb.append(", max(To_Char (ca14.POE.created_dttm, 'YYYY-MM-DD HH24:MI:SS') )              as F_Creacion ");
			sb.append(",To_Char (ca14.pol.comm_dlvr_dttm, 'dd/MM/yy')AS ETA ");
			sb.append(", min(ca14.POE.created_dttm)        as FechaLibera ");
			sb.append(", ca14.ORDERS.O_FACILITY_ALIAS_ID   as Facility ");
			sb.append(", ca14.Po.Tc_Purchase_Orders_Id     as SolCliente ");
			sb.append(", ca14.ORDERS.tc_order_id           as DO ");
			sb.append(", ca14.ORDERS.DO_STATUS             AS Est_DO ");
			sb.append(", ca14.po.order_category            AS T_Orden ");
			sb.append(", ca14.POE.old_value                AS Ant_Est ");
			sb.append(", ca14.POE.new_value                AS Nuev_Est ");

			sb.append("FROM CA14.purchase_orders_event POE ");
			sb.append("LEFT JOIN CA14.PURCHASE_ORDERs PO ON POE.purchase_orders_id = PO.purchase_orders_id  ");
			sb.append("LEFT JOIN CA14.orders ON PO.tc_purchase_orders_id = orders.purchase_order_number ");
			sb.append("LEFT JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON POE.purchase_orders_id = pol.purchase_orders_id ");

			sb.append("WHERE (ca14.POE.field_name = 'STATUS' OR POE.field_name = 'LINE ITEM STATUS') ");
			sb.append("AND ca14.ORDERS.DO_STATUS BETWEEN 110 AND 130 ");
			sb.append("AND ca14.po.channel_type = 20 ");
			sb.append("AND ca14.PO.creation_type =30 ");
			sb.append("AND ca14.PO.RMA_STATUS is NULL ");
			sb.append("AND ca14.POL.allocated_qty <> 0 ");
			sb.append("AND ca14.ORDERS.Is_cancelled = 0 ");
			sb.append("AND ca14.ORDERS.O_FACILITY_ALIAS_ID in ('012','200','400') ");
			sb.append("AND ca14.POE.created_dttm >= to_date('"+iFechaIni+" 00:00:00','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND ca14.POE.created_dttm <= to_date('"+iFechaFin+" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("GROUP BY ca14.PO.purchase_orders_id, ca14.PO.created_dttm, ca14.PO.customer_code, ca14.Po.Customer_Firstname, ca14.Po.Customer_Lastname, ca14.ORDERS.O_FACILITY_ALIAS_ID, ca14.Po.Tc_Purchase_Orders_Id, ca14.ORDERS.tc_order_id, ");
			sb.append("ca14.PO.PURCHASE_ORDERS_STATUS, ca14.ORDERS.DO_STATUS, ca14.POE.old_value, ca14.POE.new_value, ca14.po.order_category, ca14.pol.comm_dlvr_dttm ");


			info("[sb]"+sb);

			pstmt         = dbconnOracle.prepareStatement(sb.toString());
			//pstmt.setInt(1, iFechaIni);
			//pstmt.setInt(2, iFechaFin);
			sb = new StringBuffer();
			ResultSet rs = pstmt.executeQuery();
			bw  = new BufferedWriter(new FileWriter(file1));
			bw.write("OrdenNum;");
			bw.write("Est_SC;");
			bw.write("F_Creacion;");
			bw.write("ETA;");
			bw.write("FechaLibera;");

			bw.write("Facility;");
			bw.write("SolCliente;");
			bw.write("DO;");
			bw.write("Est_DO;");
			bw.write("T_Orden;");
			bw.write("Ant_Est;");
			bw.write("Nuev_Est;");
			
			bw.write("DO;");
			bw.write("SC;");
			bw.write("Estado\n");
			
			while (rs.next()) {
				bw.write(rs.getString("OrdenNum") + ";");
				bw.write(rs.getString("Est_SC") + ";");
				bw.write(rs.getString("F_Creacion") + ";");
				bw.write(rs.getString("ETA") + ";");
				bw.write(rs.getString("FechaLibera") + ";");
				bw.write(rs.getString("Facility") + ";");
				bw.write(rs.getString("SolCliente") + ";");
				bw.write(rs.getString("DO") + ";");
				bw.write(rs.getString("Est_DO") + ";");
				bw.write(rs.getString("T_Orden") + ";");
				bw.write(rs.getString("Ant_Est") + ";");
				bw.write(rs.getString("Nuev_Est") + ";");
				
				
				pstmt2.setInt(1, rs.getInt("OrdenNum"));
				pstmt2.setInt(2, rs.getInt("Est_SC"));
				pstmt2.setString(3, rs.getString("F_Creacion"));
				pstmt2.setString(4, rs.getString("ETA"));
				pstmt2.setString(5, rs.getString("FechaLibera"));
				pstmt2.setString(6, rs.getString("Facility"));
				pstmt2.setString(7, rs.getString("SolCliente"));
				pstmt2.setString(8, rs.getString("DO"));
				pstmt2.setString(9, rs.getString("Est_DO"));
				pstmt2.setString(10, rs.getString("T_Orden"));
				pstmt2.setString(11, rs.getString("Ant_Est"));
				pstmt2.setString(12, rs.getString("Nuev_Est"));
		
					
				if (rs.getString("DO") != null) {
					
					
					//bw.write(ejecutarQuery2(limpiarCeros(rs.getString("DO")), rs.getString("cant_guias"),ejecutarQueryDetalle(limpiarCeros(rs.getString("LOAD_NBR")), rs.getString("cant_guias"), rs.getString("mod_date_time2"), dbconnection), rs.getString("mod_date_time2"), dbconnection, pstmt2));
					bw.write(ejecutarQuery2(limpiarCeros(rs.getString("DO")), dbconnection, pstmt2));
					pstmt2.executeUpdate();
					commit(dbconnOracleKpi,"COMMIT");	
				}
			}

			info("Archivos creados.");
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnection,pstmt,bw);
			cerrarTodo(dbconnOracle, null, bw2);
		}
	}

	private static String ejecutarQuery2(String do_eom, Connection dbconnection,PreparedStatement pstmt2) {

		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;

		try {

			sb = new StringBuffer();
			sb.append("SELECT pkt_dtl.pkt_ctrl_nbr AS DO, pkt_dtl.BATCH_NBR AS SC FROM pkt_dtl WHERE pkt_dtl.pkt_ctrl_nbr IN ('"+do_eom+"') " );
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			sb = new StringBuffer();

			boolean reg = false;
			do{
				reg = rs.next();
				if (reg){
					sb.append( rs.getString("DO") + ";");
					sb.append( rs.getString("SC") + ";");
					sb.append("1" + "\n");
					
					
					pstmt2.setString(13, rs.getString("DO"));
					pstmt2.setString(14, rs.getString("SC"));
					pstmt2.setInt(15, 1);//"Encontrado en JDA"
					
					
					
					break;
				}else{
					sb.append("-" + ";");
					sb.append("-" + ";");
					sb.append("0" + "\n");
					
					pstmt2.setString(13, "-");
					pstmt2.setString(14, "-");
					pstmt2.setInt(15, 0);
				}
			}
			while (reg);
		}
		catch (Exception e) {

			e.printStackTrace();
			info("[crearTxt2]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null,pstmt,null);
		}
		return sb.toString();
	}
	/*
	private static String ejecutarQueryDetalle(String ctl, String cant_guias, String iFechaIni2 ,Connection dbconnection) {

		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		String result = "";

		try {

			sb = new StringBuffer();
			sb.append("Select NUMTRANSAC, NUMTIENDA, CDWMS, sum(cantidad) as detalle, sum(cantidad) as cantidad from exisbugd.exiff94 where FECTRANSAC = "+iFechaIni2+" AND NUMCARGA = "+ ctl + " group by NUMTRANSAC, NUMTIENDA, CDWMS" );
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			sb = new StringBuffer();
			

			boolean reg = false;
			do{
				reg = rs.next();
				if (reg){
					//sb.append(rs.getString("cantidad") + ";");
					result = rs.getString("cantidad");
					
					break;
				}else{
					result = "-";
				}
			}
			while (reg);
		}
		catch (Exception e) {

			e.printStackTrace();
			info("[crearTxt2]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null,pstmt,null);
		}
		return result;
	}
	*/
	
	/**
	 * Metodo que ejecuta la eliminacion de registros en una tabla 
	 * 
	 * @param Connection, conexion de las base de datos
	 * @param String, query para la eliminacion  
	 * @return 
	 */
	private static void eliminar(Connection dbconnection,  String sql) {
		info("[sql Eliminar] " + sql);
		PreparedStatement pstmt = null;
		try {
			pstmt = dbconnection.prepareStatement(sql);
			System.out.println("registros elimnados Cuadraturas : " + pstmt.executeUpdate());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			cerrarTodo(null, pstmt, null);
		}
		
	}
	
	/**
	 * Metodo que hace commit en la base de datos
	 * 
	 * @param Connection, conexion a la base de datos
	 * @return si valor de retorno
	
	*/
	private static void commit(Connection dbconnection,  String sql) {
		PreparedStatement pstmt = null;
		try {
			pstmt = dbconnection.prepareStatement(sql);
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			cerrarTodo(null, pstmt, null);
		}
	}
	 
	private static Connection crearConexion() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svbbr:1521:REPORTMHN","CONWMS","CONWMS");

		}
		catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}

	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			//Shareplex
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");

			// El servidor g500603sv0zt corresponde a Producción. por el
			// momento
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP", "ca14", "Manhattan1234");

		} catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}
	
	private static Connection crearConexionOracleKpi() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@172.18.163.15:1521:XE","kpiweb","kpiweb");

		}
		catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}
	

	private static String limpiarCeros(String str) {

		int iCont = 0;

		while (str.charAt(iCont) == '0') {

			iCont++;
		}
		return str.substring(iCont, str.length());
	}

	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:"+e.getMessage());
		}
	}

	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "dd-MM-yyyy";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	private static String restarDias2(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyy/MM/dd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	private static String restarDiasTxt(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	private static String restarDiasDel(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyy-MM-dd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
}
