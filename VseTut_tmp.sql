/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Чезганов Алексей
 * Дата: 3.12.25
*/

/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/

--CREATE TABLE ds_ecom.product_user_features AS
WITH top_regions AS -- определяю топ-3 региона по количеству заказов
    (
        SELECT
            region
        FROM (
            SELECT
                u.region,
                COUNT(DISTINCT o.order_id)                                    AS order_count,
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT  o.order_id) DESC) AS rank
            FROM ds_ecom.orders    AS o
                JOIN ds_ecom.users AS u USING (buyer_id)
            WHERE o.order_status IN ('Доставлено', 'Отменено')
            GROUP BY u.region
             ) AS ranked_regions
        WHERE rank <= 3
    ),
    filtered_orders AS -- фильтрую заказы. Топ-3 региона и нужные статусы
    (
        SELECT
            o.order_id,
            u.user_id,
            u.region,
            o.order_status,
            o.order_purchase_ts
        FROM ds_ecom.orders    AS o
            JOIN ds_ecom.users AS u USING (buyer_id)
        WHERE u.region IN (SELECT region FROM top_regions) AND o.order_status IN ('Доставлено', 'Отменено')
    ),
    order_cost AS -- вынес подсчет итоговой стоимости + учитываю стоимость доставки
    (
        SELECT
            order_id,
            SUM(price + delivery_cost) AS order_total_cost
        FROM ds_ecom.order_items
        GROUP BY order_id
    ),
    order_ratings AS -- аналогично вынес рейтинги для нужных заказов
    (
        SELECT
            fo.order_id,
            ro.review_score
        FROM filtered_orders                AS fo
            LEFT JOIN ds_ecom.order_reviews AS ro USING (order_id)
    ),
    payment_info AS
    (
        SELECT
            order_id,
            MAX( CASE WHEN (payment_type = 'денежный перевод' OR payment_type = 'банковская карта')
                AND payment_sequential = 1 THEN 1 ELSE 0 END )                AS used_money_transfer,
            MAX( CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END )       AS used_installments,
            MAX( CASE WHEN payment_type = 'промокод'THEN 1 ElSE 0 END )       AS used_promocode

        FROM ds_ecom.order_payments
        GROUP BY order_id
    ),
    user_info_stats AS ( -- предварительный сбор по пользователю и региону
        SELECT
            -- основная агрегация
            fo.user_id,
            fo.region,

            -- временная активность
            MIN(fo.order_purchase_ts)                                                 AS first_order_ts,
            MAX(fo.order_purchase_ts)                                                 AS last_order_ts,
            EXTRACT( day FROM MAX(fo.order_purchase_ts) - MIN(fo.order_purchase_ts) ) AS lifetime,

            -- информация о заказах
            COUNT(DISTINCT fo.order_id)                                               AS total_orders,
            COUNT( DISTINCT CASE WHEN ro.review_score IS NOT NULL
                THEN fo.order_id END )                                                AS num_orders_with_rating,
            AVG(ro.review_score) AS avg_order_rating, -- игнорирует null
            COUNT( DISTINCT CASE WHEN fo.order_status = 'Отменено'
                THEN fo.order_id END )                                                AS num_canceled_orders,

            -- инфрмация о платежах
            SUM( CASE WHEN fo.order_status = 'Доставлено'
                THEN oc.order_total_cost ELSE 0 END )                                 AS total_order_costs,
            COUNT( DISTINCT CASE WHEN pi.used_installments = 1
                THEN fo.order_id END )                                                AS num_installment_orders,
            COUNT( DISTINCT CASE WHEN pi.used_promocode = 1
                THEN fo.order_id END )                                                AS num_orders_with_promo,

            -- бинарные признаки
            MAX(pi.used_money_transfer)                                               AS used_money_transfer,
            MAX(pi.used_installments)                                                 AS used_installments,
            MAX( CASE WHEN fo.order_status = 'Отменено'
                THEN 1 ELSE 0 END )                                                   AS used_cancel

        FROM filtered_orders        AS fo
            LEFT JOIN order_ratings AS ro USING (order_id)
            LEFT JOIN order_cost    AS oc USING (order_id)
            LEFT JOIN payment_info  AS pi USING (order_id)
        GROUP BY fo.user_id, fo.region
    )
SELECT
    user_id,
    region,
    first_order_ts,
    last_order_ts,
    COALESCE(lifetime, 0)                                   AS lifetime,
    total_orders,
    COALESCE( ROUND(avg_order_rating::numeric, 2), -1 )     AS avg_order_rating, -- -1 значит не оценивал
    num_orders_with_rating,
    num_canceled_orders,
    ROUND(num_canceled_orders::numeric / total_orders, 2)   AS canceled_orders_ratio,
    ROUND(total_order_costs::numeric, 2)                    AS total_order_costs,
    ROUND(total_order_costs::numeric / total_orders, 2)     AS avg_order_cost,
    num_installment_orders,
    num_orders_with_promo,
    COALESCE(used_money_transfer, 0)                        AS used_money_transfer,
    COALESCE(used_installments, 0)                          AS used_installments,
    COALESCE(used_cancel, 0)                                AS used_cancel
FROM user_info_stats;

-- позаботился о замене null'ов, тк dm для модели.
-- tid: 01f2285f85a1c603eb7ef755ad311769


/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

SELECT
    segment,
    COUNT(*)                        AS total_users,
    ROUND( AVG(total_orders), 2 )   AS avg_orders_per_segment,
    ROUND( AVG(avg_order_cost), 2 ) AS avg_order_cost_seg
FROM (
        SELECT
            user_id,
            total_orders,
            avg_order_cost,
            CASE
                WHEN total_orders = 1 THEN '1 заказ'
                WHEN total_orders BETWEEN 2 AND 5 THEN '2 - 5 заказов'
                WHEN total_orders BETWEEN 6 AND 10 THEN '6 - 10 заказов'
                WHEN total_orders >= 11 THEN '11+ заказов'
            END AS segment
        FROM ds_ecom.product_user_features
     ) AS segmented
GROUP BY segment;

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 Большинство пользователей совершили только 1 заказ (60 468), также в этом сегменте высокая средняя стоимость заказа (3 324.08).
 В сегменте (2-5) наблюдается сильный спад по количеству пользователей (1 934), средняя стоимость заказа удерживается. (3 091.36)
 Остальные сегменты сохраняют общую тенденцию: Чем больше заказов, тем в сегменте меньше клиентов и средний чек также становится меньше.
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

SELECT
    user_id,
    region,
    total_orders,
    avg_order_cost
FROM ds_ecom.product_user_features
WHERE total_orders >= 3
ORDER BY avg_order_cost DESC
LIMIT 15;

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 Топ-15 покупателей преимущественно находятся в Москве. Средний чек сильно превышается, ~3-3.5 раза.
 При этом, количество заказов небольшое, 3-5 заказов.
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

SELECT
    region,
    COUNT(*)                                                                      AS total_users,
    SUM(total_orders)                                                             AS total_orders,
    ROUND( AVG(avg_order_cost), 2 )                                               AS avg_order_cost,
    ROUND( SUM(num_installment_orders)::numeric / SUM(total_orders), 2 )          AS installment_order_ratio,
    ROUND( SUM(num_orders_with_promo)::numeric / SUM(total_orders), 2 )           AS promo_orders_ratio,
    ROUND( COUNT( CASE WHEN used_cancel = 1 THEN 1 END )::numeric / COUNT(*), 2 ) AS users_wt_cancel_ratio
FROM ds_ecom.product_user_features
GROUP BY region
ORDER BY total_users DESC;

/* Напишите краткий комментарий с выводами по результатам задачи 3.
   В выводе я выделяю Москву и остальные регионы, так как у Петербурга и Новосибирской области показатели отличаются незначительно
 Количество клиентов и, соответственно, заказов было совершено в Москве (39 386) и (40 747), что в 4 раза отличается от
 двух других регионов по обоим показателям. Однако, средняя стоимость заказа отличается, на 13% меньше.
   Количество отмен в Москве и Петербугре составляет 1%, в то время как в Новосибирской области близко к 0.
   Доля пользователей промокодами составляет 4% стабильно во всех регионах.
   В Москве меньше половины заказов без рассрочки (48%), в то время как Петербугре и Новосибирской области больше (54-55%)
*/


/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

SELECT
    EXTRACT(month FROM first_order_ts)                                                    AS month,
    COUNT(*)                                                                              AS new_users,
    SUM(total_orders)                                                                     AS total_orders,
    ROUND( AVG(avg_order_cost), 2 )                                                       AS avg_order_cost,
    ROUND( AVG(avg_order_rating), 2 )                                                     AS avg_rating,
    ROUND( COUNT( CASE WHEN used_money_transfer = 1 THEN 1 END )::numeric / COUNT(*), 2 ) AS money_used_ratio,
    ROUND( AVG( EXTRACT(day FROM lifetime) ), 0 )                                         AS avg_lifetime_days
FROM ds_ecom.product_user_features
WHERE EXTRACT(year FROM first_order_ts) = 2023
GROUP BY EXTRACT(month FROM first_order_ts)
ORDER BY EXTRACT(month FROM first_order_ts)

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 Группы пользователей по месяцам 2023 года демонстрируют рост числа новых клиентов с (465) в январе до (2 832) в октябре
 при увеличении общего количества заказов с (499) до (2954). Средний чек колеблется в диапазоне примерно (2 580–3 310),
 достигая максимума в сентябре, при стабильном высоком среднем рейтинге (4.14–4.32). Доля заказов с денежным переводом
 держится на уровне (0.19–0.22), а средний lifetime по группам снижается с 13 дней в январе до 4–5 дней к осени.
 */