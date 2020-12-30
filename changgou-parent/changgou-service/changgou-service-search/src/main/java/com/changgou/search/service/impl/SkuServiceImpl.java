package com.changgou.search.service.impl;

import com.alibaba.fastjson.JSON;
import com.changgou.goods.feign.SkuFeign;
import com.changgou.goods.pojo.Sku;
import com.changgou.search.dao.SkuEsMapper;
import com.changgou.search.pojo.SkuInfo;
import com.changgou.search.service.SkuService;
import entity.Result;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.util.StringUtil;

import java.util.*;

/**
 * 描述
 *
 * @author www.itheima.com
 * @version 1.0
 * @package com.changgou.search.service.impl *
 * @since 1.0
 */
@Service
public class SkuServiceImpl implements SkuService {


    @Autowired
    private SkuFeign skuFeign;

    @Autowired
    private SkuEsMapper skuEsMapper;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;


    @Override
    public void importEs() {
        //1.调用 goods微服务的fegin 查询 符合条件的sku的数据
        Result<List<Sku>> skuResult = skuFeign.findByStatus("1");
        List<Sku> data = skuResult.getData();//sku的列表
        //将sku的列表 转换成es中的skuinfo的列表
        List<SkuInfo> skuInfos = JSON.parseArray(JSON.toJSONString(data), SkuInfo.class);
        for (SkuInfo skuInfo : skuInfos) {
            //获取规格的数据  {"电视音响效果":"立体声","电视屏幕尺寸":"20英寸","尺码":"165"}

            //转成MAP  key: 规格的名称  value:规格的选项的值
            Map<String, Object> map = JSON.parseObject(skuInfo.getSpec(), Map.class);
            skuInfo.setSpecMap(map);
        }

        // 2.调用spring data elasticsearch的API 导入到ES中
        skuEsMapper.saveAll(skuInfos);
    }

    /**
     * 关键字搜索
     *
     * @param searchMap
     * @return
     */
    @Override
    public Map search(Map<String, String> searchMap) {

        //条件构造器
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();

        //关键字搜索
        AggregatedPage<SkuInfo> skuInfos = getSkuInfos(searchMap, builder);

        //返回结果集
        Map map = getMap(skuInfos);

        //分类搜索
        if (searchMap == null || StringUtil.isEmpty(searchMap.get("categoryName"))) {
            List<String> categoryNameList = getCategoryName(searchMap, builder);
            map.put("categoryNameList", categoryNameList);
        }

        //品牌搜索
        if (searchMap == null || StringUtil.isEmpty(searchMap.get("brandName"))) {
            List<String> brandNameList = getBrandName(searchMap, builder);
            map.put("brandNameList", brandNameList);
        }
        //规格搜索
        Map<String, Set<String>> spcMap = getSpcMap(builder);

        map.put("spcMap", spcMap);

        return map;
    }


    /**
     * 搜索关键字查询
     *
     * @param searchMap
     * @param builder
     * @return
     */
    public AggregatedPage<SkuInfo> getSkuInfos(Map<String, String> searchMap, NativeSearchQueryBuilder builder) {
        if (searchMap != null && searchMap.size() > 0) {
            //1.获取关键字的值.
            String keywords = searchMap.get("keywords");
            String categoryName = searchMap.get("categoryName");
            String brandName = searchMap.get("brandName");

            //聚合条件
            BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();

            //关键字
            if (!StringUtil.isEmpty(keywords)) {
                // builder.withQuery(QueryBuilders.queryStringQuery(keywords).field("name"));
                booleanQuery.must(QueryBuilders.queryStringQuery(keywords).field("name"));
            }
            //分类
            if (!StringUtil.isEmpty(categoryName)) {
                booleanQuery.must(QueryBuilders.termQuery("categoryName",categoryName));
            }
            //品牌
            if (!StringUtil.isEmpty(brandName)) {
                booleanQuery.must(QueryBuilders.termQuery("name", brandName));
            }
            //规格
            if (searchMap != null) {
                for (String key : searchMap.keySet()) {
                    if (key.startsWith("spc_")) {   //遍历查询条件map，查询spc_开头得数值
                        booleanQuery.must(QueryBuilders.termQuery("specMap." + key.substring(5) + ".keyword", searchMap.get(key)));
                    }
                }
            }
        }

        //执行查询
        return elasticsearchTemplate.queryForPage(builder.build(), SkuInfo.class);
    }


    /**
     * 搜索分类查询
     *
     * @param builder
     * @return
     */
    public List<String> getCategoryName(Map<String, String> searchMap, NativeSearchQueryBuilder builder) {
        builder.addAggregation(AggregationBuilders.terms("skuCategoryGroup").field("categoryName"));

        AggregatedPage<SkuInfo> skuInfos1 = elasticsearchTemplate.queryForPage(builder.build(), SkuInfo.class);

        StringTerms categoryName1 = (StringTerms) skuInfos1.getAggregation("skuCategoryGroup");


        List<String> categoryNameList = new ArrayList<>();
        //遍历循环
        if (categoryName1 != null) {
            for (StringTerms.Bucket bucket : categoryName1.getBuckets()) {
                String categoryName = bucket.getKeyAsString();
                categoryNameList.add(categoryName);
            }
        }
        return categoryNameList;
    }


    /**
     * 搜品牌查询
     *
     * @param builder
     * @return
     */
    public List<String> getBrandName(Map<String, String> searchMap, NativeSearchQueryBuilder builder) {
        builder.addAggregation(AggregationBuilders.terms("skuBrandName").field("brandName"));
        AggregatedPage<SkuInfo> skuInfos1 = elasticsearchTemplate.queryForPage(builder.build(), SkuInfo.class);

        StringTerms brandNameString = (StringTerms) skuInfos1.getAggregation("skuBrandName");


        List<String> brandNameList = new ArrayList<>();
        //遍历循环
        if (brandNameString != null) {
            for (StringTerms.Bucket bucket : brandNameString.getBuckets()) {
                String brandName = bucket.getKeyAsString();
                brandNameList.add(brandName);
            }
        }
        return brandNameList;
    }


    /**
     * 封装结果集
     *
     * @param skuInfos
     * @return
     */
    public Map getMap(AggregatedPage<SkuInfo> skuInfos) {
        //获取总记录数
        long totalElements = skuInfos.getTotalElements();
        //获取内容
        List<SkuInfo> content = skuInfos.getContent();
        //获取总页数
        int totalPages = skuInfos.getTotalPages();

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("rows", content);
        resultMap.put("total", totalElements);
        resultMap.put("totalPages", totalPages);
        return resultMap;
    }


    /**
     * 获取规格集合
     *
     * @param builder
     * @return
     */
    private Map<String, Set<String>> getSpcMap(NativeSearchQueryBuilder builder) {

        builder.addAggregation(AggregationBuilders.terms("skuSpec").field("spec.keyword"));
        AggregatedPage<SkuInfo> skuInfos = elasticsearchTemplate.queryForPage(builder.build(), SkuInfo.class);

        StringTerms categoryName = (StringTerms) skuInfos.getAggregation("skuSpec");

        //创建结果集对象
        Map<String, Set<String>> setMap = new HashMap<>();


        //遍历规格搜查结果
        if (categoryName != null) {
            for (StringTerms.Bucket bucket : categoryName.getBuckets()) {
                String keyAsString = bucket.getKeyAsString();
                //字符串转成map
                Map<String, String> map = JSON.parseObject(keyAsString, Map.class);

                //遍历map,放入新map
                for (Map.Entry<String, String> spcName : map.entrySet()) {
                    //获取健
                    String key = spcName.getKey();

                    //获取值
                    String value = spcName.getValue();

                    //先获取值，如果存在就直接添加，不存在就新建
                    Set<String> spcSet = setMap.get(key);
                    if (spcSet == null) {
                        spcSet = new HashSet<>();
                    }
                    spcSet.add(value);
                    setMap.put(key, spcSet);
                }
            }
        }
        return setMap;
    }

}
