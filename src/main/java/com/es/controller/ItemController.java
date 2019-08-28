package com.es.controller;

import com.es.bean.Item;
import com.es.dao.ItemRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class ItemController {
    @Autowired
    ItemRepository itemRepository;
    @Autowired
    ElasticsearchTemplate esTemplate;

    /**
     * 创建索引
     * ElasticsearchTemplate中提供了创建索引的API
     */
    @GetMapping("/create/indices")
    public void createIndices() {
        // 创建索引，会根据Item类的@Document注解信息来创建
        esTemplate.createIndex(Item.class);
        // 配置映射，会根据Item类中的id、Field等字段来自动完成映射
        esTemplate.putMapping(Item.class);
    }

    /**
     * 删除索引
     */
    @GetMapping("/delete/indices")
    public void deleteIndices() {
        esTemplate.deleteIndex(Item.class);
        // 根据索引名字删除
        //esTemplate.deleteIndex("item");
    }

    /**
     * 创建单个索引
     */
    @GetMapping("/add/index")
    public void addIndex() {
        Item item = new Item(1L, "小米手机7", " 手机", "小米", 3499.00, "http://image.baidu.com/13123.jpg");
        itemRepository.save(item);
    }

    /**
     * 批量创建索引
     */
    @GetMapping("/add/index/list")
    public void addIndexList() {
        List<Item> list = new ArrayList<Item>();
        list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.baidu.com/13123.jpg"));
        // 接收对象集合，实现批量新增
        itemRepository.saveAll(list);
    }

    /**
     * 修改索引
     */
    @GetMapping("/update/index")
    public void updateIndex() {
        Item item = new Item(1L, "苹果XSMax", "手机", "小米", 3499.00, "http://image.baidu.com/13123.jpg");
        itemRepository.save(item);
    }

    /**
     * 查询所有
     */
    @GetMapping("/find/index")
    public Object queryAll() {
        // 查找所有
        //Iterable<Item> list = this.itemRepository.findAll();
        // 对某字段排序查找所有 Sort.by("price").descending() 降序
        // Sort.by("price").ascending():升序
        Iterable<Item> list = this.itemRepository.findAll(Sort.by("price").ascending());
        for (Item item : list) {
            System.out.println(item);
        }
        return list;
    }


    /**
     * 价格范围查询
     */
    @GetMapping("/find/index/by/price")
    public Object queryByPriceBetween() {
        List<Item> list = itemRepository.findByPriceBetween(2000.00, 3500.00);
        for (Item item : list) {
            System.out.println("item = " + item);
        }
        return list;
    }

    @GetMapping("/find/index/findByCategoryAndPrice")
    public Object findByNameAndPrice() {
        List<Item> list = itemRepository.findByCategoryAndPrice("手机", 3699.00);
        for (Item item : list) {
            System.out.println(item);
        }

        return list;
    }

    /**
     * match底层是词条匹配
     *
     * @return
     */
    @GetMapping("/find/index/matchQuery")
    public Object matchQuery() {
        //构建查询条件
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
        //添加分词查询
        builder.withQuery(QueryBuilders.matchQuery("title", "华为"));
        // 查询 自动分页 ,默认查找第一页的10条数据
        Page<Item> list = itemRepository.search(builder.build());
        //总条数
        System.out.println(list.getTotalElements());
        for (Item it : list) {
            System.out.println(it);
        }
        return list;
    }

    /**
     * termQuery
     *
     * @return
     */
    @GetMapping("/find/index/termQuery")
    public Object termQuery() {
        // 查询条件生成器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.termQuery("price", 3499.00));
        // 查询 自动分页 ,默认查找第一页的10条数据
        Page<Item> list = itemRepository.search(queryBuilder.build());
        for (Item it : list) {
            System.out.println(it);
        }
        return list;
    }

    /**
     * booleanQuery
     *
     * @return
     */
    @GetMapping("/find/index/booleanQuery")
    public Object booleanQuery() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", "华为")).must(QueryBuilders.matchQuery("brand", "华为")));
        //查找
        Page<Item> list = itemRepository.search(queryBuilder.build());
        System.out.println("总条数：" + list.getTotalElements());
        for (Item it : list) {
            System.out.println(it);
        }
        return list;
    }

    /**
     * 模糊查询
     *
     * @return
     */
    @GetMapping("/find/index/fuzzyQuery")
    public Object fuzzyQuery() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.fuzzyQuery("title", "faceoooo"));
        Page<Item> list = itemRepository.search(queryBuilder.build());
        System.out.println("总条数：" + list.getTotalElements());
        for (Item it : list) {
            System.out.println(it);
        }
        return list;

    }

    /**
     * 分页查询
     *
     * @return
     */
    @GetMapping("/find/index/pageSearch")
    public Object pageSearch() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "手机"));
        //分页
        int page = 0;
        int size = 3;
        queryBuilder.withPageable(PageRequest.of(page, size));
        //搜索
        Page<Item> page1 = itemRepository.search(queryBuilder.build());
        //总条数
        System.out.println("总条数：" + page1.getTotalElements());
        //总页数
        System.out.println(page1.getTotalPages());
        // 当前页
        System.out.println(page1.getNumber());
        //每页大小
        System.out.println(page1.getSize());
        //所有数据
        for (Item item : page1) {
            System.out.println(item);
        }
        return page1;
    }

    /**
     * 排序查询
     */
    @GetMapping("/find/index/searchAndSort")
    public void searchAndSort() {
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本分词查询
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "手机"));

        // 排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.ASC));

        // 搜索，获取结果
        Page<Item> items = this.itemRepository.search(queryBuilder.build());
        // 总条数
        long total = items.getTotalElements();
        System.out.println("总条数 = " + total);

        for (Item item : items) {
            System.out.println(item);
        }
    }

    /**
     * 聚合查询
     * 聚合为桶bucket--分组--类似group  by
     * 桶就是分组，比如这里我们按照品牌brand进行分组：
     */
    @GetMapping("/find/index/searchAgg")
    public Object searchAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果
        //queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
        queryBuilder.addAggregation(
                AggregationBuilders.terms("brands").field("brand"));
        // 2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) this.itemRepository.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        // 3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3、遍历
        for (StringTerms.Bucket bucket : buckets) {
            // 3.4、获取桶中的key，即品牌名称
            System.out.println(bucket.getKeyAsString());
            // 3.5、获取桶中的文档数量
            System.out.println(bucket.getDocCount());
        }
        return buckets;
    }

    /**
     * 嵌套聚合，求平均值---度量
     * 需求：求桶--分组，每个品牌手机的平均价格
     * 思路：（分组求桶）    +    求平均值(度量)
     */
    @GetMapping("/find/index/subAgg")
    public Object subAgg() {
        NativeSearchQueryBuilder queryBuilder1 = new NativeSearchQueryBuilder();
        queryBuilder1.addAggregation(AggregationBuilders.terms("brands").field("brand")
                .subAggregation(AggregationBuilders.avg("priceAvg").field("price")));

        AggregatedPage<Item> aggregatedPage = (AggregatedPage<Item>) itemRepository.search(queryBuilder1.build());

        StringTerms brands = (StringTerms) aggregatedPage.getAggregation("brands");

        List<StringTerms.Bucket> buckets = brands.getBuckets();
        for (StringTerms.Bucket bu : buckets) {
            System.out.print(bu.getKeyAsString() + "\t" + bu.getDocCount() + "\t");

            InternalAvg avg = (InternalAvg) bu.getAggregations().asMap().get("priceAvg");
            System.out.println(avg.getValue());
        }
        return  buckets;
    }
}