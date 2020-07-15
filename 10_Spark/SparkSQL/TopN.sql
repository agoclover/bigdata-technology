
--1.查询用户行为表，过滤出点击行为，和城市以及商品表进行连接查询
select 
    c.area,
    p.product_name
from 
    user_visit_action a
left join 
    city_info c
on 
    a.city_id = c.city_id
left join
    product_info p
on
    a.click_product_id = p.product_id
where 
    a.click_product_id != -1
limit 10;

--2.按照地区和商品名称分组，统计出每个商品在每个地区的总点击次数
select 
    t1.area,
    t1.product_name,
    count(*) as product_click_count
from 
    (
        select 
            c.area,
            p.product_name
        from 
            user_visit_action a
        left join 
            city_info c
        on 
            a.city_id = c.city_id
        left join
            product_info p
        on
            a.click_product_id = p.product_id
        where 
            a.click_product_id != -1
    )t1
group by t1.area,t1.product_name
limit 10;

--3.使用排序窗口函数，对每个地区内，按照点击次数降序排列
select 
    t2.area,
    t2.product_name,
    t2.product_click_count,
    ROW_NUMBER() over(partition by t2.area order by t2.product_click_count desc) cn
from 
    (
        select 
            t1.area,
            t1.product_name,
            count(*) as product_click_count
        from 
            (
                select 
                    c.area,
                    p.product_name
                from 
                    user_visit_action a
                left join 
                    city_info c
                on 
                    a.city_id = c.city_id
                left join
                    product_info p
                on
                    a.click_product_id = p.product_id
                where 
                    a.click_product_id != -1
            )t1
        group by t1.area,t1.product_name
    )t2

--4.对排序的结果取前3名
select
    t3.area,
    t3.product_name,
    t3.product_click_count,
    t3.cn
from 
    (
        select 
        t2.area,
        t2.product_name,
        t2.product_click_count,
        ROW_NUMBER() over(partition by t2.area order by t2.product_click_count desc) cn
       from 
        (
            select 
                t1.area,
                t1.product_name,
                count(*) as product_click_count
            from 
                (
                    select 
                        c.area,
                        p.product_name
                    from 
                        user_visit_action a
                    left join 
                        city_info c
                    on 
                        a.city_id = c.city_id
                    left join
                        product_info p
                    on
                        a.click_product_id = p.product_id
                    where 
                        a.click_product_id != -1
                )t1
            group by t1.area,t1.product_name
        )t2
    )t3
where cn <= 3

--5.对于城市备注，实现逻辑稍微复杂，可以通过自定义函数(弱类型)实现该功能。