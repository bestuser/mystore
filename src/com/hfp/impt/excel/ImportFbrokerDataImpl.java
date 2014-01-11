package com.hfp.impt.excel;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.afis.db.utils.DBUtils;
import com.afis.hfp.business.store.TradingManagements;
import com.afis.hfp.business.store.atom.impl.MemberManagementsDAOImpl;
import com.afis.hfp.business.store.impl.TradingManagementsImpl;
import com.afis.hfp.entities.dao.member.FBMemberGroupDAO;
import com.afis.hfp.entities.dao.member.FBfeeRateDAO;
import com.afis.hfp.entities.dao.member.FBgroupDAO;
import com.afis.hfp.entities.dao.member.FBgroupPrivilegeDAO;
import com.afis.hfp.entities.dao.member.FBprivilegePolicyDAO;
import com.afis.hfp.entities.dao.member.GroupDAO;
import com.afis.hfp.entities.dao.member.GroupPrivilegeDAO;
import com.afis.hfp.entities.dao.member.MemberGroupDAO;
import com.afis.hfp.entities.dao.trading.FeeRateDAO;
import com.afis.hfp.entities.dao.trading.PrivilegePolicyDAO;
import com.afis.hfp.entities.dao.trading.ContractDAO;
import com.afis.hfp.entities.impl.trading.FeeRateDAOImpl;
import com.afis.hfp.entities.impl.trading.PrivilegePolicyDAOImpl;
import com.afis.hfp.entities.impl.member.FBMemberGroupDAOImpl;
import com.afis.hfp.entities.impl.member.FBfeeRateDAOImpl;
import com.afis.hfp.entities.impl.member.FBgroupDAOImpl;
import com.afis.hfp.entities.impl.member.FBgroupPrivilegeDAOImpl;
import com.afis.hfp.entities.impl.member.FBprivilegePolicyDAOImpl;
import com.afis.hfp.entities.impl.member.GroupDAOImpl;
import com.afis.hfp.entities.impl.member.GroupPrivilegeDAOImpl;
import com.afis.hfp.entities.impl.member.MemberDAOImpl;
import com.afis.hfp.entities.impl.member.MemberGroupDAOImpl;
import com.afis.hfp.entities.impl.trading.ContractDAOImpl;
import com.afis.hfp.entities.model.member.FBMemberGroupExample;
import com.afis.hfp.entities.model.member.FBMemberGroupSelective;
import com.afis.hfp.entities.model.member.FBfeeRateExample;
import com.afis.hfp.entities.model.member.FBfeeRateSelective;
import com.afis.hfp.entities.model.member.FBgroup;
import com.afis.hfp.entities.model.member.FBgroupExample;
import com.afis.hfp.entities.model.member.FBgroupPrivilege;
import com.afis.hfp.entities.model.member.FBgroupPrivilegeExample;
import com.afis.hfp.entities.model.member.FBgroupPrivilegeSelective;
import com.afis.hfp.entities.model.member.FBgroupSelective;
import com.afis.hfp.entities.model.member.FBprivilegePolicyExample;
import com.afis.hfp.entities.model.member.FBprivilegePolicySelective;
import com.afis.hfp.entities.model.member.FbrokerSelective;
import com.afis.hfp.entities.model.member.Group;
import com.afis.hfp.entities.model.member.GroupExample;
import com.afis.hfp.entities.model.member.GroupPrivilege;
import com.afis.hfp.entities.model.member.GroupPrivilegeExample;
import com.afis.hfp.entities.model.member.GroupPrivilegeSelective;
import com.afis.hfp.entities.model.member.GroupSelective;
import com.afis.hfp.entities.model.member.MemberGroupExample;
import com.afis.hfp.entities.model.member.MemberGroupSelective;
import com.afis.hfp.entities.model.member.MemberSelective;
import com.afis.hfp.entities.model.trading.Contract;
import com.afis.hfp.entities.model.trading.ContractExample;
import com.afis.hfp.entities.model.trading.FeeRate;
import com.afis.hfp.entities.model.trading.FeeRateExample;
import com.afis.hfp.entities.model.trading.FeeRateSelective;
import com.afis.hfp.entities.model.trading.Goods;
import com.afis.hfp.entities.model.trading.PrivilegePolicy;
import com.afis.hfp.entities.model.trading.PrivilegePolicyExample;
import com.afis.hfp.entities.model.trading.PrivilegePolicySelective;
import com.afis.utils.Converter;
import com.ibatis.sqlmap.client.SqlMapClient;



public class ImportFbrokerDataImpl {

	private static Logger logger =  LoggerFactory.getLogger(ImportFbrokerDataImpl.class);
	private static SqlMapClient sqlMapClient = null;
	private static final String EXCHANG_ID ="001";
	private static Sheet sheet = null;
	private static List<DataRow> rows = new ArrayList<DataRow>();
	private static Map<String,List<String>> tradeFeeList = new HashMap<String,List<String>>();
//	private static Map<String,List<String>> tradeFeeList2 = new HashMap<String,List<String>>();
	private static Map<String,List<String>> returnFeeList = new HashMap<String,List<String>>();
	private static Map<String,Map<String,List<String>>> bReturnFeeList = new HashMap<String,Map<String,List<String>>>();
	private static Map<String,Map<String,List<String>>> bTradeFeeList = new HashMap<String,Map<String,List<String>>>();
	
	private static Map<String,List<String>> brokerMemberList = new HashMap<String,List<String>>();//经纪人所属会员
	
	private static Map<String,List<String>>   memberFeeList = new HashMap<String,List<String>>();
	private static Map<String,String>         memberFeeCountList = new HashMap<String,String>();
	
	private static Map<String,List<String>>   bMemberreturnList = new HashMap<String,List<String>>();
	private static Map<String,String>         bMemberreturnCountList = new HashMap<String,String>();
	
	private static Map<String,String>         feeBindGoodID2 = new HashMap<String,String>();
	private static Map<Long,String>         feeBindGoodID = new HashMap<Long,String>();
	private static Set<Long>				goodsIdSet = new HashSet<Long>();
	private static Map<Long,String>           goodIdBindDeliveryFee = new HashMap<Long,String>();
	
 	private static Map<String,List<String>> feeML = new HashMap<String,List<String>>();
 	private static Map<String,List<String>> returnML = new HashMap<String,List<String>>();
	private static Set<String> memberSet = new HashSet<String>();
	private static Map<String, String> goodsM = null;
	private static TradingManagements tradingManagements = new TradingManagementsImpl();
	private static final String RATIO = "比率";
	private static final String PREFIX = "C00";
	private static MemberManagementsDAOImpl mmDaoImpl = new MemberManagementsDAOImpl();
	private static List<String> NoFindMembers = new ArrayList<String>();
	private  MemberDAOImpl mDaoImpl = new MemberDAOImpl(sqlMapClient);
	private  ContractDAO contractDao = new ContractDAOImpl(sqlMapClient);
	private  GroupDAO groupDao = new GroupDAOImpl(sqlMapClient);
	private  FBgroupDAO fbGroupDao = new FBgroupDAOImpl(sqlMapClient);
	private  FBMemberGroupDAO fbmemGDao = new FBMemberGroupDAOImpl(sqlMapClient);
	private  FBgroupPrivilegeDAO  fbGroupPDao = new FBgroupPrivilegeDAOImpl(sqlMapClient);
	private  FBfeeRateDAO fbFeeRDao = new FBfeeRateDAOImpl(sqlMapClient);
	private  FBprivilegePolicyDAO fbPriPolDao = new FBprivilegePolicyDAOImpl(sqlMapClient);
	private  GroupPrivilegeDAO groupPrivilegeDao = new GroupPrivilegeDAOImpl(sqlMapClient);
	private  MemberGroupDAO memberGroupDao = new MemberGroupDAOImpl(sqlMapClient);
	private  PrivilegePolicyDAO privilegePDao = new PrivilegePolicyDAOImpl(sqlMapClient);
	private  FeeRateDAO feeRDao = new FeeRateDAOImpl(sqlMapClient);
	
	private static List<String> policyList = new ArrayList<String>();
	private static List<String> tradePolicyList = new ArrayList<String>();

	static{
		sqlMapClient = DBUtils.getSqlMapClient();
	}
	
	public static void main(String[] args) throws InvalidFormatException, IOException, SQLException{
		initGoods();
		SqlMapClient sqlMapClient = DBUtils.getSqlMapClient();
//		if(args.length == 0){
//			throw new IllegalArgumentException("请提供excel文档！");
//		}
		try
		{
			sqlMapClient.startTransaction();
			String file = null;
			if(args == null || args.length == 0){
				throw new Exception("请提供文件参数！");
			}else{
				file =args[0];
			}
			ImportFbrokerDataImpl imp = new ImportFbrokerDataImpl();
			logger.info("准备导入......");
			//imp.loadExcel("20130102特许服务商手续费返还.xls");
			imp.loadExcel(file);
			imp.processing();		
			for(String name : policyList){
				System.out.println(name);
			}
			
			logger.info("共有{}个经纪人",brokerMemberList.size());
			logger.info("共有{}个会员需要被绑定经纪人",memberSet.size());	
			logger.info("共有{}个会员不存在:{}",NoFindMembers.size(),NoFindMembers);
			logger.info("共创建{}个返佣策略方案",policyList.size());
			logger.info("共创建{}个交易策略方案",tradePolicyList.size());
			
			sqlMapClient.commitTransaction();
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		finally
		{
			try
			{
				sqlMapClient.endTransaction();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	private boolean loadExcel(String fileName) throws Exception{
		InputStream is = null;
		
			is = this.getClass().getClassLoader().getResourceAsStream(fileName);
			if( is == null ){
					is = new FileInputStream(fileName);
			}
		Workbook workbook = null;		
		try {
			workbook = WorkbookFactory.create(is);
			sheet = workbook.getSheetAt(0);
		} catch (InvalidFormatException | IOException e) {
			e.printStackTrace();
		}
		if(sheet != null)
			return true;
		else
			return false;
	}
	
	public List<DataRow> processing() throws Exception {
		for(int i =2;i<=sheet.getLastRowNum();i++){
			Row row = sheet.getRow(i);
			DataRow  dataRow = new DataRow();
			String brokerId = row.getCell(0).getStringCellValue();//经纪人ID号
			String bindMemberId = PREFIX + row.getCell(0).getStringCellValue();//经纪人自身会员号
			String subMemberId = (PREFIX + row.getCell(1).toString()).split("\\.").length>0?PREFIX + row.getCell(1).toString().split("\\.")[0]:PREFIX + row.getCell(1).toString();//从属会员号
			String openCloseFee = row.getCell(2).toString();//开平仓手续费
			String openCloseFeeType = row.getCell(3).getStringCellValue();//开平仓费用比率、固定
			String closeTodayFee = row.getCell(4).getStringCellValue();//平今手续费
			String closeTodayFeeType = row.getCell(5).getStringCellValue();//平今手续费比率、固定
			String returnFee = row.getCell(6).toString();//返佣手续费
			String returnFeeType = row.getCell(7).getStringCellValue();//返佣手续费类型
			String goodType = row.getCell(8).getStringCellValue();//品种
			
			dataRow.setBrokerID(brokerId);
			dataRow.setBindMemeber(bindMemberId);
			dataRow.setMemberId(subMemberId);
			dataRow.setOpenCloseFee(openCloseFee.contains("%")?(Converter.getLong(Converter.getDouble(openCloseFee.substring(0, openCloseFee.length()-1))*100)):(Converter.getLong(Converter.getDouble(openCloseFee)*100)));
			dataRow.setOpenCloseFeeTag(RATIO.equals(openCloseFeeType)? "1" : "0");
			dataRow.setCloseTodayFee(openCloseFee.contains("%")?(Converter.getLong(Converter.getDouble(closeTodayFee.substring(0, closeTodayFee.length()-1))*100)):(Converter.getLong(Converter.getDouble(closeTodayFee)*100)));
			dataRow.setCloseTodayFeeTag(RATIO.equals(closeTodayFeeType)? "1" : "0");
			dataRow.setBrokerFee(returnFee.contains("%")?(Converter.getLong(Converter.getDouble(returnFee.substring(0, returnFee.length()-1))*100)):(Converter.getLong(Converter.getDouble(returnFee)*100)));
			dataRow.setBrokerFeeTag(RATIO.equals(returnFeeType)? "1" : "0");
			dataRow.setGoodType(goodType);
			
			
			memberSet.add(subMemberId);
			//商品交收费率
			String gdsId = goodsM.get(goodType);
			System.out.println(goodType +":::::::"+ gdsId);
			goodsIdSet.add(Converter.getLong(gdsId));
			
			
			
			if(bTradeFeeList.get(dataRow.getBrokerID()) != null){
				Map<String,List<String>> tradeFeeList1 = bTradeFeeList.get(dataRow.getBrokerID());
				//交易策略
				String key =""+dataRow.getOpenCloseFee()+"*"+dataRow.getOpenCloseFeeTag()+"*"+dataRow.getCloseTodayFee()+"*"+dataRow.getCloseTodayFeeTag()+"*"+dataRow.getGoodType();
				List<String> memberList = tradeFeeList1.get(key);
				if(memberList != null){
					memberList.add(dataRow.getMemberId());
				}else{
					memberList = new ArrayList<String>();
					memberList.add(dataRow.getMemberId());
					tradeFeeList1.put(key, memberList);				
				}
			}else{
				Map<String,List<String>> tradeFeeList1 = new HashMap<String,List<String>>();
				//交易策略
				String key =""+dataRow.getOpenCloseFee()+"*"+dataRow.getOpenCloseFeeTag()+"*"+dataRow.getCloseTodayFee()+"*"+dataRow.getCloseTodayFeeTag()+"*"+dataRow.getGoodType();
				List<String> memberList = tradeFeeList1.get(key);
				if(memberList != null){
					memberList.add(dataRow.getMemberId());
				}else{
					memberList = new ArrayList<String>();
					memberList.add(dataRow.getMemberId());
					tradeFeeList1.put(key, memberList);							
				}
				bTradeFeeList.put(dataRow.getBrokerID(), tradeFeeList1);
			}			
			
			//交易策略
			String key =""+dataRow.getOpenCloseFee()+"*"+dataRow.getOpenCloseFeeTag()+"*"+dataRow.getCloseTodayFee()+"*"+dataRow.getCloseTodayFeeTag()+"*"+dataRow.getGoodType();
			List<String> memberList = tradeFeeList.get(key);
			if(memberList != null){
				memberList.add(dataRow.getMemberId());
			}else{
				memberList = new ArrayList<String>();
				memberList.add(dataRow.getMemberId());
				tradeFeeList.put(key, memberList);				
			}
			
			
			if(bReturnFeeList.get(dataRow.getBrokerID()) != null){
				Map<String,List<String>> returnFeeList1 = bReturnFeeList.get(dataRow.getBrokerID());
				//返佣策略
				String key3 =""+dataRow.getBrokerFee()+"*"+dataRow.getBrokerFeeTag()+"*"+dataRow.getGoodType();
				List<String> memberList3 = returnFeeList1.get(key3);
				if(memberList3 != null){
					memberList3.add(dataRow.getMemberId());
				}else{
					memberList3 = new ArrayList<String>();
					memberList3.add(dataRow.getMemberId());
					returnFeeList1.put(key3, memberList3);				
				}
			}else{
				Map<String,List<String>> returnFeeList1 = new HashMap<String,List<String>>();
				//返佣策略
				String key3 =""+dataRow.getBrokerFee()+"*"+dataRow.getBrokerFeeTag()+"*"+dataRow.getGoodType();
				List<String> memberList3 = returnFeeList1.get(key3);
				if(memberList3 != null){
					memberList3.add(dataRow.getMemberId());
				}else{
					memberList3 = new ArrayList<String>();
					memberList3.add(dataRow.getMemberId());
					returnFeeList1.put(key3, memberList3);				
				}
				bReturnFeeList.put(dataRow.getBrokerID(), returnFeeList1);
			}	
			
			//返佣策略
			String key3 =""+dataRow.getBrokerFee()+"*"+dataRow.getBrokerFeeTag()+"*"+dataRow.getGoodType();
			List<String> memberList3 = returnFeeList.get(key3);
			if(memberList3 != null){
				memberList3.add(dataRow.getMemberId());
			}else{
				memberList3 = new ArrayList<String>();
				memberList3.add(dataRow.getMemberId());
				returnFeeList.put(key3, memberList3);				
			}
			
			//经纪人下所有会员
			List<String> fbmemberList = brokerMemberList.get(dataRow.getBrokerID());
			if(fbmemberList == null){
				fbmemberList = new ArrayList<String>();
				fbmemberList.add(dataRow.getMemberId());
				brokerMemberList.put(dataRow.getBrokerID(), fbmemberList);
			}else{
				if(!fbmemberList.contains(dataRow.getMemberId()))				
					fbmemberList.add(dataRow.getMemberId());
			}
			
		}		
		clearOldData();
		bindGoodsDeliveryFee();
 		System.out.println(goodIdBindDeliveryFee);
		processingTradeAndMemberGroup2();
	 	processingBrokerAndGroup2();	 
		return rows;
	}


	private void clearOldData() throws SQLException {
		
		MemberGroupExample arg0 = new MemberGroupExample();
		arg0.createCriteria().andGroupIdIsNotNull();
		memberGroupDao.deleteByExample(arg0 );
		
		GroupPrivilegeExample arg2 = new GroupPrivilegeExample();
		arg2.createCriteria().andGroupIdIsNotNull();
		groupPrivilegeDao.deleteByExample(arg2 );
		
		GroupExample arg1 = new GroupExample();
		arg1.createCriteria().andIdIsNotNull();
		// TODO Auto-generated method stub
		groupDao.deleteByExample(arg1);
		

		
		PrivilegePolicyExample arg3 = new PrivilegePolicyExample();
		arg3.createCriteria().andNameLike("%[%]%");
		privilegePDao.deleteByExample(arg3 );
		
		FeeRateExample arg4 = new FeeRateExample();
		arg4.createCriteria().andNameLike("%[%]%");
		feeRDao.deleteByExample(arg4 );
		
		
		
		FBMemberGroupExample arg5 = new FBMemberGroupExample();
		arg5.createCriteria().andGroupIdIsNotNull();
		fbmemGDao.deleteByExample(arg5 );
		
		FBgroupPrivilegeExample arg7 = new FBgroupPrivilegeExample();
		arg7.createCriteria().andGroupIdIsNotNull();
		fbGroupPDao.deleteByExample(arg7 );
		
		FBgroupExample arg6 = new FBgroupExample();
		arg6.createCriteria().andIdIsNotNull();
		// TODO Auto-generated method stub
		fbGroupDao.deleteByExample(arg6);
		

		
		FBprivilegePolicyExample arg8 = new FBprivilegePolicyExample();
		arg8.createCriteria().andNameLike("%[%]%");
		fbPriPolDao.deleteByExample(arg8 );
		
		FBfeeRateExample arg9 = new FBfeeRateExample();
		arg9.createCriteria().andNameLike("%[%]%");
		fbFeeRDao.deleteByExample(arg9 );
		
	}


	private void bindGoodsDeliveryFee() throws Exception {
		Iterator<Long> itr = goodsIdSet.iterator();
		while(itr.hasNext()){
			Long id = itr.next();
			ContractExample exmp = new ContractExample();
			exmp.createCriteria().andGoodsIdEqualTo(Converter.getString(id));
			exmp.orderByEndDay("desc");
			List<Contract> list = contractDao.selectByExample(exmp);
			if(list == null || list.size() == 0)
				throw new Exception("商品ID[ "+id+" ]找不到相应的交收费率！");
				//logger.info("商品ID[ "+id+" ]找不到相应的交收费率！");
			else{
				PrivilegePolicyExample pExmp = new PrivilegePolicyExample();
				pExmp.createCriteria().andIdEqualTo(list.get(0).getPrivilegeId()).andTypeEqualTo("3");
				List<PrivilegePolicy> privilegeList = privilegePDao.selectByExample(pExmp);
				if(privilegeList == null || privilegeList.size() == 0)
					throw new Exception("商品ID[ "+id+" ]找不到相应的交收费率！");
					//logger.info("商品ID[ "+id+" ]找不到相应的交收费率！");
				else{
					privilegeList.get(0).getRefId();
					FeeRateExample frExmp = new FeeRateExample();
					frExmp.createCriteria().andIdEqualTo(privilegeList.get(0).getRefId()).andItemIdEqualTo(13l);
					List<FeeRate> feeR = feeRDao.selectByExample(frExmp);
					if(feeR == null || feeR.size() == 0 )
						throw new Exception("商品ID[ "+id+" ]找不到相应的交收费率！");
						//.info("商品ID[ "+id+" ]找不到相应的交收费率！");
					goodIdBindDeliveryFee.put(id, feeR.get(0).getBuy()+"*"+feeR.get(0).isBuyRate()+"*"+feeR.get(0).getBuy()+"*"+feeR.get(0).isBuyRate());
				}
			}
		}
	}


	
	

	private void bMGroupByFeeAndBindPrivilege2(String broker) throws SQLException {		
		//经纪人会员按返佣策略分组
		Set<Entry<String, String>> memberFeeCountSet = bMemberreturnCountList.entrySet();
	 	Iterator<Entry<String,String>> itr = memberFeeCountSet.iterator();
	 	
	 	returnML = new HashMap<String,List<String>>();
	 	String k = null;
	 	String v = null;
	 	while(itr.hasNext()){
	 		Entry<String,String> ent = itr.next();
	 		k = ent.getKey();
	 		v = ent.getValue();
	 		List<String> l = returnML.get(ent.getValue());
	 		if(l != null){
	 			l.add(k);
	 		}else{
	 			l = new ArrayList<String>();
	 			l.add(k);
	 			returnML.put(v, l);
	 		}
	 	}
	 	
	 	//经纪人会员和策略组绑定
	 	//会员组创建
		FBgroupSelective g = new FBgroupSelective();
		long memberGroupId = 0;
		
		Set<Entry<String, List<String>>> FeeMLSet = returnML.entrySet();
	 	Iterator<Entry<String, List<String>>> feeMLSetitr = FeeMLSet.iterator();
	 	int tmpN =0;
	 	List<String> bindFeeStringList = null;
	 	while(feeMLSetitr.hasNext()){	 		
	 			Entry<String, List<String>> ent = feeMLSetitr.next();
	 			String key = ent.getKey();
	 			List<String> val  = ent.getValue();
	 			Collections.sort(val);
	 			String mems = val.get(0)+"~"+val.get(val.size() - 1 );
	 			//++++++++++++++++++++++++++++++++
	 			
	 			FBgroupExample fbgExmaple = new FBgroupExample();
	 			fbgExmaple.createCriteria().andNameEqualTo(broker+"["+mems+"]"+"客户组");
				//是否存在经纪人客户组
	 			List<FBgroup> fbgl = fbGroupDao.selectByExample(fbgExmaple);
	 			if(fbgl != null && fbgl.size() > 0 ){
	 				for(FBgroup xyz : fbgl){
		 				Long id = fbgl.get(0).getId();
		 				FBMemberGroupExample fbmgExample = new FBMemberGroupExample();
		 				fbmgExample.createCriteria().andGroupIdEqualTo(id);
						//删除组下关联会员
		 				fbmemGDao.deleteByExample(fbmgExample);
		 				FBgroupPrivilegeExample fbgpExample = new FBgroupPrivilegeExample();
		 				fbgpExample.createCriteria().andGroupIdEqualTo(id);
		 				List<FBgroupPrivilege> pList = fbGroupPDao.selectByExample(fbgpExample );//策略方案IDList
		 				List<Long> longList = new ArrayList<Long>();
		 				for(FBgroupPrivilege f: pList){
		 					longList.add(f.getProvilegeId());
		 				}
		 				FBfeeRateExample fbfeerateExample = new FBfeeRateExample();
		 				fbfeerateExample.createCriteria().andIdIn(longList);
						//删除策略方案下的策略
		 				fbFeeRDao.deleteByExample(fbfeerateExample );
		 				//删除绑定表
		 				fbGroupPDao.deleteByExample(fbgpExample);
		 				FBprivilegePolicyExample fbprivilegePExampel = new FBprivilegePolicyExample();
		 				fbprivilegePExampel.createCriteria().andIdIn(longList);
						//删除策略方案
		 				fbPriPolDao.deleteByExample(fbprivilegePExampel );
		 				//删除经纪人会员组
		 				fbGroupDao.deleteByExample(fbgExmaple);
	 				}
	 			}
	 			
	 			//++++++++++++++++++++++++++++++++
				//添加一个会员组
				g.setName(broker+"["+mems+"]"+"客户组");
				g.setType("2");//经纪人
				memberGroupId = mmDaoImpl.fbGroupAdd(g);
				//添加会员到组中
				FBMemberGroupSelective mg = null;
				System.out.println(broker+"["+mems+"]"+"客户组");
				for(int y=0;y<val.size();y++){
					mg = new FBMemberGroupSelective();
					mg.setGroupId(memberGroupId);
					mg.setMemberId(val.get(y));	
					mg.setType("2");
					try{
						System.out.println(val.get(y)+"准备编入组");
						mmDaoImpl.memberInFbgroup(mg);
					}catch(Exception ex){
						NoFindMembers.add(val.get(y));
						System.out.println(val.get(y)+"准备编入组-error-忽略该会员");
						//ex.printStackTrace();						
					}
				}		
				
				bindFeeStringList = bMemberreturnList.get(val.get(0));
				//创建返佣费率，创建策略，创建绑定。
			 	//绑定策略方案				
				//绑定方案到会员组
				
				
				
			 	for(int y =0;y<bindFeeStringList.size();y++){
			 		try{
			 			//创建返佣费率
			 			String ret = bindFeeStringList.get(y);
			 			String[] arrays = ret.split("\\#");
			 			for(int yy =0 ;yy<arrays.length;yy++){
			 				if("".equals(arrays[yy])){
			 					continue;
			 				}
			 				
			 				String[] returnKArray =  arrays[yy].split("\\*");
							FBfeeRateSelective fbfrSelective = new FBfeeRateSelective();
							fbfrSelective.setBuy(Converter.getLong(returnKArray[0]));
							fbfrSelective.setBuyRate(returnKArray[1].equals("0")?false:true);
							fbfrSelective.setSell(Converter.getLong(returnKArray[0]));
							fbfrSelective.setSellRate(returnKArray[1].equals("0")?false:true);
							System.out.println(returnKArray[1].equals("0")?false:true);
							fbfrSelective.setItemId(33l);
							fbfrSelective.setName(broker+"["+mems+"]费率["+returnKArray[2]+"]");
							fbfrSelective.setValue(1l);
							long fbfeeRateId = mmDaoImpl.fbfeeRateAdd(fbfrSelective);
							//create t_m_fbprivilege_policy
							FBprivilegePolicySelective fbpgSelective = new FBprivilegePolicySelective();
							fbpgSelective.setName(broker+"["+mems+"]策略["+returnKArray[2]+"]");
							fbpgSelective.setRefId(fbfeeRateId);
							fbpgSelective.setType("3");
							long fbppId = mmDaoImpl.fbprivilegePolicyAdd(fbpgSelective);
							policyList.add(broker+"["+mems+"]策略["+returnKArray[2]+"]");
							FBgroupPrivilegeSelective gpS = new FBgroupPrivilegeSelective();
							gpS.setExchangeId(EXCHANG_ID);
							gpS.setGroupId(memberGroupId);//会员组
							gpS.setProvilegeId(fbppId);//策略方案
							System.out.println(feeBindGoodID2.get(ret)+"=商品ID");
							gpS.setRefId(feeBindGoodID2.get(ret));//设置商品Id	
							gpS.setType("1");
							mmDaoImpl.FBgroupBindPrivilege(gpS);
							//会员-会员组-策略方案-会员组策略方案绑定
			 			}

			 		}
			 		catch(Exception ex){
			 			ex.printStackTrace();
			 			throw ex;
			 		}
			 	}
				tmpN++;
	 	}
	}	

	/**
	 * 创建交易策略和会员组,交易策略绑定会员组
	 * @throws SQLException
	 */
	private void processingTradeAndMemberGroup2() throws SQLException {
		Set<Entry<String, Map<String, List<String>>>> brokerSet = bTradeFeeList.entrySet();
		Iterator<Entry<String, Map<String, List<String>>>> outItr = brokerSet.iterator();
		while(outItr.hasNext()){
			Entry<String, Map<String, List<String>>> bTradeFee = outItr.next();
			Set<Entry<String, List<String>>> set = bTradeFee.getValue().entrySet();
			Iterator<Entry<String, List<String>>> itr = set.iterator();
			List<String> tFeeList = null;
			int ii = 0;	
			memberFeeList = new HashMap<String,List<String>>();
			memberFeeCountList = new HashMap<String,String>();
			
			while(itr.hasNext()){				
				
				Entry<String, List<String>> entry = itr.next();
				String eKey = entry.getKey();
				List<String> eValue = entry.getValue();//经纪人会员List
				Collections.sort(eValue);
				String start = eValue.get(0);
				String end = eValue.get(eValue.size()-1);
				String mems = "["+start +"~"+ end+"]";
				//添加交易策略
				String[] tradeArray = eKey.split("\\*");

				//交易策略列表-会员
				for(int z =0;z<eValue.size();z++){
					String memId = eValue.get(z);
					tFeeList = memberFeeList.get(memId);
					String count = memberFeeCountList.get(memId);
					if(tFeeList != null){
						tFeeList.add(eKey);//添加交易策略
					}else{
						tFeeList = new ArrayList<String>();
						tFeeList.add(eKey);					
					}
					memberFeeList.put(memId, tFeeList);
					if(count != null){
						count=count +"#"+ eKey;
					}else{
						count = "";
						count=count+"#"+eKey;
					}
					memberFeeCountList.put(memId, count);
				}
				//交易策略id列表-商品			
				String goodId = goodsM.get(tradeArray[4]);//获取商品对应ID
				feeBindGoodID2.put(eKey, goodId);			
				ii++;
				//break;
				
			}
			mGroupByFeeAndBindPrivilege2(bTradeFee.getKey());
		}
	}

	private void mGroupByFeeAndBindPrivilege2(String brokerId) throws SQLException {
		//同一经纪人下的会员按交易策略分组
		Set<Entry<String, String>> memberFeeCountSet = memberFeeCountList.entrySet();
	 	Iterator<Entry<String,String>> itr = memberFeeCountSet.iterator();
	 	List<String> memberList = brokerMemberList.get(brokerId);
	 	Collections.sort(memberList);
	 	feeML = new HashMap<String,List<String>>();
	 	String k = null;
	 	String v = null;
	 	while(itr.hasNext()){
	 		Entry<String,String> ent = itr.next();
	 		k = ent.getKey();
	 		v = ent.getValue();
	 		List<String> l = feeML.get(ent.getValue());
	 		if(l != null){
	 			l.add(k);
	 		}else{
	 			l = new ArrayList<String>();
	 			l.add(k);
	 			feeML.put(v, l);
	 		}
	 	}
	 	

	 	//会员和策略组绑定
	 	//会员组创建
		GroupSelective g = new GroupSelective();
		long memberGroupId = 0;
		
		Set<Entry<String, List<String>>> FeeMLSet = feeML.entrySet();
	 	Iterator<Entry<String, List<String>>> feeMLSetitr = FeeMLSet.iterator();
	 	int tmpN =0;
	 	List<String> bindFeeStringList = null;
	 	while(feeMLSetitr.hasNext()){
 	 			Entry<String, List<String>> ent = feeMLSetitr.next();
	 			String key = ent.getKey();
	 			List<String> val  = ent.getValue();
	 			Collections.sort(val);
	 		 	String startMem = val.get(0);
	 		 	String endMem = memberList.get(val.size()-1);
	 		 	String mems = "["+startMem +"~"+ endMem+"]";
	 		 	
	 		 	//++++++++++++++++++++++++++
	 		 	checkAndDelOldData(brokerId,mems);
	 		 	
	 		 	GroupExample gExample = new GroupExample();
	 		 	gExample.createCriteria().andNameEqualTo(brokerId+mems+"客户组");
				 List<Group> groupL = groupDao.selectByExample(gExample);
				 
				 if(groupL != null & groupL.size()>0){
					 for(Group xyz : groupL){
						 //删除会员和组的关联数据
						 Long id = xyz.getId();
						 MemberGroupExample mGExample = new MemberGroupExample();
						 mGExample.createCriteria().andGroupIdEqualTo(id);
						 int i = memberGroupDao.deleteByExample(mGExample);
						 GroupPrivilegeExample groupPrivExample = new GroupPrivilegeExample();
						 groupPrivExample.createCriteria().andGroupIdEqualTo(id);
						 //查询客户组和交易策略绑定表,绑定多个交易策略
						 List<GroupPrivilege> gpL = groupPrivilegeDao.selectByExample(groupPrivExample );
						 List<Long> idList = new ArrayList<Long>();
						 for(GroupPrivilege gp : gpL){
							 idList.add(gp.getProvilegeId());
						 }
						 FeeRateExample tFeeRExample = new FeeRateExample();					 
						 tFeeRExample.createCriteria().andIdIn(idList);
						//删除交易策略
						 feeRDao.deleteByExample(tFeeRExample);
						 //删除组策略绑定表数据
						 groupPrivilegeDao.deleteByExample(groupPrivExample);
						 //删除交易策略方案
						 for(Long x : idList)
							 tradingManagements.privilegePolicyDelete(x);
						 //删除会员组
						 mmDaoImpl.groupDel(id);
					 }
				 }
	 		 	
				 
				 
				 
				 //++++++++++++++++++++++++++
				//添加一个会员组
				g.setName(brokerId+mems+"客户组");
				
				System.out.println(brokerId+mems+"客户组");
				
				memberGroupId = mmDaoImpl.groupAdd(g);
				//添加会员到组中
				MemberGroupSelective mg = null;
				
				for(int y=0;y<val.size();y++){
					mg = new MemberGroupSelective();
					mg.setGroupId(memberGroupId);
					mg.setMemberId(val.get(y));	
					
					try{
						System.out.println(val.get(y)+"准备编入组");
						mmDaoImpl.memberInGroup(mg);
					}catch(Exception ex){
						NoFindMembers.add(val.get(y));
						System.out.println(val.get(y)+"准备编入组-error-忽略该会员");
						//ex.printStackTrace();						
					}
				}		
				
				bindFeeStringList = memberFeeList.get(val.get(0));
				//创建费率，策略，绑定
			 	//绑定策略方案				
				//绑定方案到会员组
			 	for(int y =0;y<bindFeeStringList.size();y++){
			 		try{
			 			String feeString = bindFeeStringList.get(y);
			 			String[] tradeArray = feeString.split("\\*");
			 			
			 			
						//添加交易策略
						List<FeeRateSelective> feeList = new ArrayList<FeeRateSelective>();
						
						FeeRateSelective feeSOpen = new FeeRateSelective();	

						feeSOpen.setName(brokerId+"["+mems+"]"+"开仓费率"+"["+tradeArray[4]+"]");
						feeSOpen.setItemId(1l);
						
						FeeRateSelective feeSClose = new FeeRateSelective();
						feeSClose.setName(brokerId+"["+mems+"]"+"平仓费率"+"["+tradeArray[4]+"]");
						feeSClose.setItemId(3l);
						
						FeeRateSelective feeSCloseToday = new FeeRateSelective();
						feeSCloseToday.setName(brokerId+"["+mems+"]"+"平今费率"+"["+tradeArray[4]+"]");
						feeSCloseToday.setItemId(2l);
						
						FeeRateSelective feeDelivery = new FeeRateSelective();
						feeDelivery.setName(brokerId+"["+mems+"]"+"交收费率"+"["+tradeArray[4]+"]");
						feeDelivery.setItemId(13l);
						
						feeSOpen.setBuy(Converter.getLong(tradeArray[0]));
						feeSOpen.setBuyRate(tradeArray[1].equals("0")? false:true);
						feeSOpen.setSell(Converter.getLong(tradeArray[0]));
						feeSOpen.setSellRate(tradeArray[1].equals("0")? false:true);
						feeList.add(feeSOpen);
						
						feeSClose.setBuy(Converter.getLong(tradeArray[0]));
						feeSClose.setBuyRate(tradeArray[1].equals("0")? false:true);
						feeSClose.setSell(Converter.getLong(tradeArray[0]));
						feeSClose.setSellRate(tradeArray[1].equals("0")? false:true);
						feeList.add(feeSClose);
						
						feeSCloseToday.setBuy(Converter.getLong(tradeArray[2]));
						feeSCloseToday.setBuyRate(tradeArray[3].equals("0")? false:true);
						feeSCloseToday.setSell(Converter.getLong(tradeArray[2]));
						feeSCloseToday.setSellRate(tradeArray[3].equals("0")? false:true);
						feeList.add(feeSCloseToday);
						
						
						String deliveryString = goodIdBindDeliveryFee.get(Converter.getLong(feeBindGoodID2.get(feeString)));
						System.out.println(feeBindGoodID2.get(feeString));
						String[] deli = deliveryString.split("\\*");
						
						feeDelivery.setBuy(Converter.getLong(deli[0]));
						feeDelivery.setBuyRate(deli[1].equals("false")? false : true);
						feeDelivery.setSell(Converter.getLong(deli[2]));
						feeDelivery.setSellRate(deli[3].equals("false")? false : true);
						feeList.add(feeDelivery);
						
						long feesId = tradingManagements.feesAdd(feeList, true);
						
						//添加交易策略方案
						List<PrivilegePolicySelective> ppSList = new ArrayList<PrivilegePolicySelective>();
						PrivilegePolicySelective ppS = new PrivilegePolicySelective();
						ppS.setRefId(feesId);//费率ID
						ppS.setName(brokerId+mems+"交易策略方案["+tradeArray[4]+"]");
						ppS.setType("3");
						ppSList.add(ppS);
						long provilegeId = tradingManagements.privilegePolicyAdd(ppSList);//交易策略方案Id;
			 			
						tradePolicyList.add(brokerId+mems+"交易策略方案["+tradeArray[4]+"]");
						
						GroupPrivilegeSelective gpS = new GroupPrivilegeSelective();
						gpS.setExchangeId(EXCHANG_ID);
						gpS.setGroupId(memberGroupId);//会员组
						gpS.setProvilegeId(provilegeId);//策略方案
						System.out.println(feeBindGoodID2.get(feeString)+"商品ID");
						gpS.setRefId(feeBindGoodID2.get(feeString));//设置商品Id	
						gpS.setType("1");
						mmDaoImpl.groupPrivilegeAdd(gpS);
						//会员-会员组-策略方案-会员组策略方案绑定
			 		}
			 		catch(Exception ex){
			 			ex.printStackTrace();
			 			throw ex;
			 		}
			 	}
				tmpN++;
	 	}

	}
	
		
	

	private void checkAndDelOldData(String brokerId, String mems) {
		// TODO Auto-generated method stub
		
	}


	/**
	 * 创建经纪人，创建经纪人会员组
	 * @throws SQLException
	 */
	private void processingBrokerAndGroup2() throws SQLException {
		
		Set<Entry<String, List<String>>> set = brokerMemberList.entrySet();
		Iterator<Entry<String, List<String>>> itr = set.iterator();
		while(itr.hasNext()){//经纪人
			Entry<String, List<String>> entry = itr.next();
			String eKey = entry.getKey();			
			if(createBroker(eKey,PREFIX+eKey)){//创建经纪人
				List<String> eValue = entry.getValue();
				for(int x=0;x<eValue.size();x++){//会员绑定经纪人
					String memberId = eValue.get(x);
					MemberSelective mSelective = new MemberSelective();					
					mSelective.setMemberId(memberId);
					mSelective.setOtherFbrokerId(eKey);
					mDaoImpl.updateByPrimaryKey(mSelective);
				}
			}
		}
		
		Set<Entry<String, Map<String, List<String>>>> outSet = bReturnFeeList.entrySet();
		Iterator<Entry<String, Map<String, List<String>>>> outItr = outSet.iterator();
		while(outItr.hasNext()){
			//返佣费率
			Entry<String, Map<String, List<String>>>  oEntry = outItr.next();
			String brokerId = oEntry.getKey() ;
			Set<Entry<String, List<String>>> set2 = oEntry.getValue().entrySet();
			Iterator<Entry<String, List<String>>> itr2 = set2.iterator();
		int i = 0;
		bMemberreturnList = new HashMap<String,List<String>>();
		bMemberreturnCountList =new HashMap<String,String>();
		while(itr2.hasNext()){
			Entry<String, List<String>> entry = itr2.next();
			String eKey = entry.getKey();
			List<String> eValue = entry.getValue();
			
//			//create t_m_fbfee_rate
			String[] returnKArray = eKey.split("\\*");
			List<String> tFeeList = null;
			//返佣列表-会员
			for(int z =0;z<eValue.size();z++){
				String memId = eValue.get(z);
				tFeeList = bMemberreturnList.get(memId);
				String count = bMemberreturnCountList.get(memId);
				if(tFeeList != null){
					tFeeList.add(eKey);//添加返佣策略
				}else{
					tFeeList = new ArrayList<String>();
					tFeeList.add(eKey);					
				}
				bMemberreturnList.put(memId, tFeeList);
				if(count != null){
					count = count +"#"+ eKey;
				}else{
					count = "";
					count = count +"#"+ eKey;
				}
				bMemberreturnCountList.put(memId, count);
			}
		
			//返佣策略id列表-商品			
			String goodId = goodsM.get(returnKArray[2]);//获取商品对应ID
			feeBindGoodID2.put(eKey, goodId);	//费率和商品绑定		
			i++;			
		}
		
		bMGroupByFeeAndBindPrivilege2(brokerId);
		}
	}	
	

	private static Map<String, String> initGoods() throws SQLException {
		List<Goods> goodsList = tradingManagements.getGoodsTree();
		Map<String,String> goods = new HashMap<String,String>();
		for(int good = 0;good<goodsList.size();good++){
			Goods goodz = goodsList.get(good);
			goods.put(goodz.getName(), goodz.getGoodsId());
		}
		goodsM = goods;
		return goods;
	}

	public boolean createBroker(String brokerId,String memberName) throws SQLException {
		// TODO Auto-generated method stub
		FbrokerSelective selective = new FbrokerSelective();
		selective.setFbrokerId(brokerId);
		selective.setName(brokerId);
		selective.setType("2");
		selective.setMemberId(PREFIX + brokerId);
		selective.setStatus("1");
		try{
			mmDaoImpl.fbrokerAdd(selective);
		}
		catch(Exception ex){
			if(mmDaoImpl.fbroker(brokerId) != null){
				//经纪人已经存在
			}
			else{
				return false;
			}			
		}
		return true;
	}

	public long createFBFeeRate(DataRow dataRow) throws SQLException {
		// TODO Auto-generated method stub
		FBfeeRateSelective selective = new FBfeeRateSelective();
		selective.setBuy(dataRow.getBrokerFee());
		selective.setBuyRate(dataRow.getBrokerFeeTag().equals("0")? false:true);
		selective.setSell(dataRow.getBrokerFee());
		selective.setSellRate(dataRow.getBrokerFeeTag().equals("0")? false:true);
		mmDaoImpl.fbfeeRateAdd(selective);
		return 0;
	}


	public long createFbGroup(String groupName) throws SQLException  {
		// TODO Auto-generated method stub
		FBgroupSelective selective = new FBgroupSelective();
		selective.setName(groupName);
		selective.setType("2");
		return mmDaoImpl.fbGroupAdd(selective);
	}


	
}
