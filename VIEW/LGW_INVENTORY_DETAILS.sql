CREATE OR REPLACE FORCE VIEW "CNL_SYS"."LGW_INVENTORY_DETAILS" ("TOTAL_QTY_IN_WMS", "QTY_AVAILABLE_NO_SUSPENSE", "QTY_RESERVED", "QTY_ALLOCATED", "QTY_IN_SUSPENSE_POS_NEG", "ORD_QTY_NOT_ALLOCATED_IN_WMS", "QTY_AVAILABLE_MIN_SUSPENSE", "SKU_ID", "CLIENT_ID", "SITE_ID", "OWNER_ID") AS 
  WITH my_data AS (
      SELECT
         SUM(i.qty_on_hand) total_qty_in_wms,
         SUM(i.qty_allocated) qty_allocated_inv,
         SUM(i.qty_on_hand - i.qty_allocated) qty_without_allocate_inv,
         nvl(s.qty_on_hand, 0) suspense_qty,
         nvl(p.qty_allocated, 0) pick_face_alloc_qty,
         nvl(m.qty_to_move, 0) replenish_qty,
         nvl(l.qty_pending, 0) qty_non_alloc_order,
         i.sku_id,
         i.client_id,
         i.site_id,
         i.owner_id
      FROM      
                                                -- INVENTORY TABLE       
         (
            SELECT
               SUM(y.qty_on_hand) qty_on_hand,
               SUM(y.qty_allocated) qty_allocated,
               y.sku_id,
               y.client_id,
               y.site_id,
               y.owner_id
            FROM
               (
                  SELECT
                     o.qty_on_hand,
                     DECODE(o.loc_type, 'Shipdock', o.qty_on_hand, 'Trailer', o.qty_on_hand, o.qty_allocated) qty_allocated,
                     o.sku_id,
                     o.client_id,
                     o.site_id,
                     o.owner_id
                  FROM
                     (
                        SELECT
                           SUM(inv.qty_on_hand) qty_on_hand,
                           SUM(inv.qty_allocated) qty_allocated,
                           inv.sku_id,
                           inv.client_id,
                           inv.site_id,
                           inv.owner_id,
                           loc.loc_type
                        FROM
                           dcsdba.inventory   inv,
                           dcsdba.location    loc
                        WHERE
                           loc.location_id = inv.location_id
                           AND loc.site_id = inv.site_id
                           AND inv.location_id != 'SUSPENSE'
			   AND inv.lock_status != 'Locked'
                        GROUP BY
                           inv.sku_id,
                           inv.client_id,
                           inv.site_id,
                           inv.owner_id,
                           loc.loc_type
                     ) o
               ) y
            GROUP BY
               y.sku_id,
               y.client_id,
               y.site_id,
               y.owner_id
         ) i
                                                -- SUSPENSE TABLE
         LEFT JOIN (
            SELECT
               SUM(sp.qty_on_hand) qty_on_hand,
               sp.sku_id,
               sp.client_id,
               sp.site_id,
               sp.owner_id
            FROM
               dcsdba.inventory sp
            WHERE
               sp.location_id = 'SUSPENSE'
            GROUP BY
               sp.sku_id,
               sp.client_id,
               sp.site_id,
               sp.owner_id
         ) s ON s.site_id = i.site_id
                AND s.sku_id = i.sku_id
                AND s.client_id = i.client_id
                AND s.owner_id = i.owner_id
                                                -- ORDER_LINE TABLE
         LEFT JOIN (
            SELECT
               SUM(l.qty_ordered - l.qty_tasked - l.qty_picked) qty_pending,
               l.sku_id,
               l.client_id,
               l.site_id,
               l.owner_id
            FROM
               (
                  SELECT
                     SUM(nvl(l.qty_ordered, 0)) qty_ordered,
                     SUM(nvl(l.qty_tasked, 0)) qty_tasked,
                     SUM(nvl(l.qty_picked, 0)) qty_picked,
                     SUM(nvl(l.qty_shipped, 0)) qty_shipped,
                     l.sku_id,
                     l.client_id,
                     h.owner_id,
                     h.from_site_id site_id,
                     h.status
                  FROM
                     dcsdba.order_line     l
                     INNER JOIN dcsdba.order_header   h ON h.order_id = l.order_id
                                                         AND h.client_id = l.client_id
                  GROUP BY
                     l.sku_id,
                     l.client_id,
                     h.owner_id,
                     h.from_site_id,
                     h.status
               ) l
            WHERE
               l.status NOT IN (
                  'Cancelled',
                  'Shipped',
                  'Delivered',
                  'Complete'
               )
            GROUP BY
               l.sku_id,
               l.client_id,
               l.site_id,
               l.owner_id
         ) l ON l.sku_id = i.sku_id
                AND l.client_id = i.client_id
                AND l.site_id = i.site_id
                AND l.owner_id = i.owner_id
                                                -- PICK FACE TABLE
         LEFT JOIN (
            SELECT
               SUM(pf.qty_allocated) qty_allocated,
               pf.sku_id,
               pf.client_id,
               pf.site_id,
               pf.owner_id
            FROM
               dcsdba.pick_face pf
            GROUP BY
               pf.sku_id,
               pf.client_id,
               pf.site_id,
               pf.owner_id
         ) p ON p.site_id = i.site_id
                AND p.sku_id = i.sku_id
                AND p.client_id = i.client_id
                AND p.owner_id = i.owner_id
                                                -- REPLENISHMENT QTY
         LEFT JOIN (
            SELECT
               SUM(mt.qty_to_move) qty_to_move,
               mt.sku_id,
               mt.client_id,
               mt.site_id,
               mt.owner_id
            FROM
               dcsdba.move_task mt
            WHERE
               mt.task_type = 'R'
               AND mt.task_id = 'REPLENISH'
            GROUP BY
               mt.sku_id,
               mt.client_id,
               mt.site_id,
               mt.owner_id
         ) m ON m.site_id = i.site_id
                AND m.sku_id = i.sku_id
                AND m.client_id = i.client_id
                AND m.owner_id = i.owner_id
      GROUP BY
         nvl(s.qty_on_hand, 0),
         nvl(p.qty_allocated, 0),
         nvl(m.qty_to_move, 0),
         nvl(l.qty_pending, 0),
         i.sku_id,
         i.client_id,
         i.site_id,
         i.owner_id
   )
   SELECT
      total_qty_in_wms,
      qty_without_allocate_inv - pick_face_alloc_qty + replenish_qty - qty_non_alloc_order qty_available_no_suspense,
      qty_non_alloc_order   qty_reserved,
      qty_allocated_inv + pick_face_alloc_qty - replenish_qty qty_allocated,
      suspense_qty          qty_in_suspense_pos_neg,
      qty_non_alloc_order   ord_qty_not_allocated_in_wms,
      qty_without_allocate_inv - pick_face_alloc_qty + replenish_qty - qty_non_alloc_order + DECODE(instr(TO_CHAR(suspense_qty), '-'
      ), 0, 0, suspense_qty) - DECODE(instr(TO_CHAR(suspense_qty), '-'), 0, suspense_qty, 0) qty_available_min_suspense,
      sku_id,
      client_id,
      site_id,
      owner_id
   FROM
      my_data