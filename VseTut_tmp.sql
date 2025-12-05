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

--CREATE TABLE ds_ecom.data_mart AS
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
        -- [!!!] проверить фильтрацию
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
        -- [!!!] Проверить фильтрацию
        SELECT
            order_id,
            MAX(CASE WHEN (payment_type = 'денежный перевод' OR payment_type = 'банковская карта')
                AND payment_sequential = 1 THEN 1 ELSE 0 END)               AS used_money_transfer,
            MAX(CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END)       AS used_installments,
            MAX(CASE WHEN payment_type = 'промокод'THEN 1 ElSE 0 END)       AS used_promocode

        FROM ds_ecom.order_payments
        GROUP BY order_id
    ),
    user_info_stats AS ( -- предварительный сбор по пользователю и региону
        SELECT
            -- основная агрегация
            fo.user_id,
            fo.region,

            -- временная активность
            MIN(fo.order_purchase_ts)                                               AS first_order_ts,
            MAX(fo.order_purchase_ts)                                               AS last_order_ts,
            EXTRACT(day FROM MAX(fo.order_purchase_ts) - MIN(fo.order_purchase_ts)) AS lifetime,

            -- информация о заказах
            COUNT(DISTINCT fo.order_id)                                             AS total_orders,
            COUNT(DISTINCT CASE WHEN ro.review_score IS NOT NULL
                THEN fo.order_id END)                                               AS num_orders_with_rating,
            AVG(ro.review_score) AS avg_order_rating, -- игнорирует null
            COUNT(DISTINCT CASE WHEN fo.order_status = 'Отменено'
                THEN fo.order_id END)                                               AS num_canceled_orders,

            -- инфрмация о платежах
            SUM(CASE WHEN fo.order_status = 'Доставлено'
                THEN oc.order_total_cost ELSE 0 END)                                AS total_order_costs,
            COUNT(DISTINCT CASE WHEN pi.used_installments = 1
                THEN fo.order_id END)                                               AS num_installment_orders,
            COUNT(DISTINCT CASE WHEN pi.used_promocode = 1
                THEN fo.order_id END)                                               AS num_orders_with_promo,

            -- бинарные признаки
            MAX(pi.used_money_transfer)                                             AS used_money_transfer,
            MAX(pi.used_installments)                                               AS used_installments,
            MAX(CASE WHEN fo.order_status = 'Отменено'
                THEN 1 ELSE 0 END)                                                  AS used_cancel

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
    COALESCE(lifetime, 0)                                 AS lifetime,
    total_orders,
    COALESCE(ROUND(avg_order_rating::numeric, 2), -1)     AS avg_order_rating, -- -1 значит не оценивал
    num_orders_with_rating,
    num_canceled_orders,
    ROUND(num_canceled_orders::numeric / total_orders, 2) AS canceled_orders_ratio,
    ROUND(total_order_costs::numeric, 2)                  AS total_order_costs,
    ROUND(total_order_costs::numeric / total_orders, 2)   AS avg_order_cost,
    num_installment_orders,
    num_orders_with_promo,
    COALESCE(used_money_transfer, 0)                      AS used_money_transfer,
    COALESCE(used_installments, 0)                        AS used_installments,
    COALESCE(used_cancel, 0)                              AS used_cancel
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

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * 
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * 
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * 
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * 