package com.wwjd.canal.canaltest.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wwjd.starter.canal.annotation.CanalEventListener;
import com.wwjd.starter.canal.annotation.content.DeleteListenPoint;
import com.wwjd.starter.canal.annotation.content.InsertListenPoint;
import com.wwjd.starter.canal.annotation.content.UpdateListenPoint;
import com.wwjd.starter.canal.annotation.table.AlertTableListenPoint;
import com.wwjd.starter.canal.annotation.table.CreateIndexListenPoint;
import com.wwjd.starter.canal.annotation.table.CreateTableListenPoint;
import com.wwjd.starter.canal.annotation.table.DropTableListenPoint;
import com.wwjd.starter.canal.client.core.CanalMsg;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 注解方法测试
 *
 * @author 阿导
 * @CopyRight 万物皆导
 * @created 2018/5/28 17:31
 * @Modified_By 阿导 2018/5/28 17:31
 */
@CanalEventListener
public class MyAnnoEventListener {
	@Autowired
	private KafkaTemplate<String,String> kafkaTemplate;
	
	@InsertListenPoint
	public void onEventInsertData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
		System.out.println("======================注解方式（新增数据操作）==========================");
		List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
		for (CanalEntry.RowData rowData : rowDatasList) {
			String sql = "use " + canalMsg.getSchemaName() + ";\n";
			StringBuffer colums = new StringBuffer();
			StringBuffer values = new StringBuffer();
			rowData.getAfterColumnsList().forEach((c) -> {
				colums.append(c.getName() + ",");
				values.append("'" + c.getValue() + "',");
			});
			
			
			sql += "INSERT INTO " + canalMsg.getTableName() + "(" + colums.substring(0, colums.length() - 1) + ") VALUES(" + values.substring(0, values.length() - 1) + ");";
			System.out.println(sql);
			List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
			List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
			Map<String, Object> map = dataToMap(beforeColumnsList, afterColumnsList,canalMsg, "insert");
			System.out.println(map);
			kafkaTemplate.send("example", JSON.toJSONString(map));
		}
		System.out.println("\n======================================================");
		
	}
	
	@UpdateListenPoint
	public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
		System.out.println("======================注解方式（更新数据操作）==========================");
		List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
		for (CanalEntry.RowData rowData : rowDatasList) {
			
			String sql = "use " + canalMsg.getSchemaName() + ";\n";
			StringBuffer updates = new StringBuffer();
			StringBuffer conditions = new StringBuffer();

			//组装数据给kafka
			List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
			List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
			Map<String, Object> map = dataToMap(beforeColumnsList, afterColumnsList,canalMsg, "update");
			System.out.println(map);
			kafkaTemplate.send("example", JSON.toJSONString(map));


			rowData.getAfterColumnsList().forEach((c) -> {
				if (c.getIsKey()) {
					conditions.append(c.getName() + "='" + c.getValue() + "'");
				} else {
					updates.append(c.getName() + "='" + c.getValue() + "',");
				}
			});

			sql += "UPDATE " + canalMsg.getTableName() + " SET " + updates.substring(0, updates.length() - 1) + " WHERE " + conditions;
			System.out.println(sql);
		}
		System.out.println("\n======================================================");
	}
	
	@DeleteListenPoint
	public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
		
		System.out.println("======================注解方式（删除数据操作）==========================");
		List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
		for (CanalEntry.RowData rowData : rowDatasList) {
			
			if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
				List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
				List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
				String sql = "use " + canalMsg.getSchemaName() + ";\n";
				
				sql += "DELETE FROM " + canalMsg.getTableName() + " WHERE ";
				StringBuffer idKey = new StringBuffer();
				StringBuffer idValue = new StringBuffer();
				for (CanalEntry.Column c : rowData.getBeforeColumnsList()) {
					if (c.getIsKey()) {
						idKey.append(c.getName());
						idValue.append(c.getValue());
						break;
					}
					
					
				}
				
				sql += idKey + " =" + idValue + ";";
				System.out.println(sql);
				Map<String, Object> map = dataToMap(beforeColumnsList, afterColumnsList, canalMsg, "delete");
				kafkaTemplate.send("example",JSON.toJSONString(map));
			}
			System.out.println("\n======================================================");
			
		}
	}
	
	@CreateTableListenPoint
	public void onEventCreateTable(CanalEntry.RowChange rowChange,CanalMsg canalMsg) {
		System.out.println("======================注解方式（创建表操作）==========================");
		System.out.println("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
		System.out.println("\n======================================================");
		Map<String, Object> map = tableAlterData(rowChange,canalMsg, "createTable");
		kafkaTemplate.send("example", JSON.toJSONString(map));
	}
	
	@DropTableListenPoint
	public void onEventDropTable(CanalEntry.RowChange rowChange,CanalMsg canalMsg) {
		System.out.println("======================注解方式（删除表操作）==========================");
		System.out.println("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
		System.out.println("\n======================================================");
		Map<String, Object> map = tableAlterData(rowChange,canalMsg, "deleteTable");
		kafkaTemplate.send("example", JSON.toJSONString(map));
	}


	@AlertTableListenPoint
	public void onEventAlertTable(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
		System.out.println("======================注解方式（修改表信息操作）==========================");
		System.out.println("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
		System.out.println("\n======================================================");
		Map<String, Object> map = tableAlterData(rowChange, canalMsg,"alterTable");
		kafkaTemplate.send("example", JSON.toJSONString(map));
	}
	
	@CreateIndexListenPoint
	public void onEventCreateIndex(CanalMsg canalMsg,CanalEntry.RowChange rowChange){
		System.out.println("======================注解方式（创建索引操作）==========================");
		System.out.println("use " + canalMsg.getSchemaName()+ ";\n" + rowChange.getSql());
		System.out.println("\n======================================================");
		Map<String, Object> map = tableAlterData(rowChange,canalMsg, "createIndex");
		kafkaTemplate.send("example", JSON.toJSONString(map));
		
	}

	/**
	 * 封装dml操作数据
	 * @param beforeList
	 * @param afterList
	 * @param canalMsg
	 * @param event_op_type
	 * @return
	 */
	private Map<String,Object> dataToMap(List<CanalEntry.Column> beforeList,List<CanalEntry.Column> afterList,CanalMsg canalMsg,String event_op_type){
		Map<String,Object> map=new HashMap<>();
		Map<String,Object> map_info=new HashMap<>();
		map.put("databaseName",canalMsg.getSchemaName());
		map.put("tableName",canalMsg.getTableName());
		map.put("type",event_op_type);
		if (event_op_type.equals("insert")) {
			for (CanalEntry.Column column : afterList) {
				if (column.getValue() != null && !column.getValue().equals(""))
					map_info.put(column.getName(), column.getValue());
			}
		}else if (event_op_type.equals("delete")){
			for(CanalEntry.Column column : beforeList) {
				if (column.getValue() != null && !column.getValue().equals(""))
					map_info.put(column.getName(), column.getValue());
			}
		}else {
			for (CanalEntry.Column column : afterList) {
					map_info.put(column.getName(), column.getValue());
			}
			Map<String,Object> beforeMap=new HashMap<>();
			for (CanalEntry.Column column : beforeList) {
				if(column.getValue()!=null&&!column.getValue().equals(""))
					beforeMap.put(column.getName(), column.getValue());
			}

			map.put("beforeColumns",beforeMap);
		}
		map.put("afterColumns",map_info);
		return map;
	}

	/**
	 * 封装ddl操作数据
	 * @param rowChange
	 * @param deleteTable
	 * @return
	 */
	private Map<String,Object> tableAlterData(CanalEntry.RowChange rowChange,CanalMsg canalMsg, String deleteTable) {
		Map<String, Object> map = new HashMap<>();
		map.put("type", deleteTable);
		map.put("databaseName", rowChange.getDdlSchemaName());
		map.put("tableName",canalMsg.getTableName());
		map.put("sql", rowChange.getSql());
		return map;
	}
	
}
