package com.example.kafkametrics.repository.metrics.iif;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface IIifMetricsPgPointsRepository extends JpaRepository<IifMetricsPgPoints, Long>, IIifMetricsPgPointsRepositoryCustom {

    @Query(value = """
            SELECT r.asset_product_ent_cd  AS assetProductEntCd,
                   p.agency_nbr            AS agencyNbr,
                   SUM(pts.pg_points_value) AS totalPgPoints
            FROM   iif_metric_points pts
            JOIN   iif_metrics_raw r
                   ON  r.agreement_product_nbr = pts.agreement_product_nbr
                   AND r.asset_id IS NOT DISTINCT FROM pts.asset_id
                   AND r.eff_end_dt            = :highDate
            JOIN   policy_aor p
                   ON  p.agreement_product_number = pts.agreement_product_nbr
            WHERE  pts.eff_end_dt = :highDate
            GROUP BY r.asset_product_ent_cd, p.agency_nbr
            """, nativeQuery = true)
    List<PgPointsAggregate> aggregateByProductAndAgency(@Param("highDate") Instant highDate);
}
