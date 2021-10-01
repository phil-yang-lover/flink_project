package com.yang.gmall;

/**
 * @Author: TODO philver-yang
 * @Date: TODO 2021/5/5
 * @Time: TODO 18:52
 * @Description: TODO :
 */
public class SugarController {
    /*@RequestMapping("/trademark")
public String getProductStatsByTrademark(@RequestParam(value = "date",defaultValue = "0") Integer date,
                                         @RequestParam(value = "limit",defaultValue = "20") Integer limit){
    if (date == 0) date=now();
    List<ProductStats> productStatsByTrademark =
        productStatsService.getProductStatsByTrademark(date, limit);

    StringBuilder stringBuilder = new StringBuilder();
    List<String> listName = new ArrayList<>();
    List<BigDecimal> amountList = new ArrayList<>();
    stringBuilder.append("{\"status\": 0,\"data\": {\"categories\": [");
    for (ProductStats productStats : productStatsByTrademark) {
        String tm_name = productStats.getTm_name();
        listName.add(tm_name);
        amountList.add(productStats.getOrder_amount());
    }

    *//*String json = "{\"status\": 0,\"data\": " +
            "{\"categories\": [\""+StringUtils.join(listName,"\",\"")+"\"]," +
            "\"series\": [{\"name\": \"商品品牌\",\"data\": ["+ StringUtils.join(amountList,",") +"]}]}}";*//*

        stringBuilder.append(StringUtils.join(listName,"\",\""));
        stringBuilder.append("],\"series\": [{\"name\": \"手机品牌\",\"data\": [");
        stringBuilder.append(StringUtils.join(amountList,","));
        stringBuilder.append("]}]}}");
        return stringBuilder.toString();
    }*/
    //拼接对象
    /*@RequestMapping("/trademark")
    public String getProductStatsByTrademark(@RequestParam(value = "date",defaultValue = "0") Integer date,
                                             @RequestParam(value = "limit",defaultValue = "20") Integer limit){

        return null;
    }

    @RequestMapping("/category3")
    public String getProductStatsCategroy3(
        @RequestParam(value = "date",defaultValue = "0") Integer date,
        @RequestParam(value = "limit",defaultValue = "10") Integer limit){

        List<ProductStats> productStatsByCategroy3 = productStatsService.getProductStatsByCategroy3(date, limit);
        StringBuilder stringBuilder = new StringBuilder
            ("{\"status\": 0,\"data\": {\"columns\": [{ \"name\": \"商品名称\",  \"id\": \"spu_name\"},{ \"name\": \"交易额\", \"id\": \"order_amount\"}\\],\"rows\": [");
        for (ProductStats productStats : productStatsByCategroy3) {
            Map hashMap = new HashMap();
            hashMap.put("spu_name",productStats.getCategory3_name());
            hashMap.put("order_amount",productStats.getOrder_amount());
            stringBuilder.append(hashMap);

        }
        *//*for (int i = 0; i < productStatsByCategroy3.size(); i++) {
            ProductStats productStats = productStatsByCategroy3.get(i);
            stringBuilder.append("{\"spu_name\": \""+productStats.getCategory3_name()+"\",\"order_amount\": \""+productStats.getOrder_amount()+"\"}");
            if (i < productStatsByCategroy3.size() - 1) {
                stringBuilder.append(",");
            }
        }*//*
        stringBuilder.append("]}}");
        return stringBuilder.toString();
    }*/
}
