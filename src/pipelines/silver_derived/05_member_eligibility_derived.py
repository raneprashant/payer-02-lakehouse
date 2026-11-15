
# ============================================================
# PAYER 360 - SILVER DERIVED
# DOMAIN 05: MEMBERSHIP & ELIGIBILITY
#
# Reads accepted Silver Conformed tables.
# Does not modify Snapshot + CDC ingestion.
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")


# ============================================================
# 1. SUBSCRIPTION MEMBERSHIP VALIDATION
# ============================================================

@dp.materialized_view(
    name=f"{CATALOG}.silver.subscription_membership_validation",
    comment="Effective-dated membership relationship validation",
)
def subscription_membership_validation():

    sm = (
        spark.read.table(
            f"{CATALOG}.silver.subscription_membership"
        )
        .filter(F.col("__END_AT").isNull())
        .alias("sm")
    )

    subs = (
        spark.read.table(
            f"{CATALOG}.silver.subscription"
        )
        .filter(F.col("__END_AT").isNull())
        .alias("s")
    )

    members = (
        spark.read.table(
            f"{CATALOG}.silver.member_master"
        )
        .filter(F.col("__END_AT").isNull())
        .alias("m")
    )

    joined = (
        sm.filter(~F.col("sm.is_deleted"))
        .join(
            subs,
            F.col("sm.subscription_id")
            == F.col("s.subscription_id"),
            "left"
        )
        .join(
            members,
            F.col("sm.member_id")
            == F.col("m.member_id"),
            "left"
        )
    )

    invalid_dates = (
        F.col("sm.relationship_start_date").isNull()
        | F.col("sm.relationship_end_date").isNull()
        | (
            F.col("sm.relationship_start_date")
            > F.col("sm.relationship_end_date")
        )
    )

    missing_subscription = (
        F.col("s.subscription_id").isNull()
        | F.coalesce(
            F.col("s.is_deleted"),
            F.lit(True)
        )
    )

    missing_member = (
        F.col("m.member_id").isNull()
        | F.coalesce(
            F.col("m.is_deleted"),
            F.lit(True)
        )
    )

    invalid_self = (
        (F.col("sm.relationship_code") == "SELF")
        & (
            F.col("sm.member_id")
            != F.col("s.subscriber_member_id")
        )
    )

    return joined.select(
        F.col("sm.subscription_member_id"),
        F.col("sm.subscription_id"),
        F.col("sm.member_id"),
        F.col("sm.relationship_code"),
        F.col("sm.relationship_start_date"),
        F.col("sm.relationship_end_date"),

        F.when(
            invalid_dates,
            "INVALID_DATES"
        )
        .when(
            missing_subscription,
            "INVALID_SUBSCRIPTION"
        )
        .when(
            missing_member,
            "INVALID_MEMBER"
        )
        .when(
            invalid_self,
            "INVALID_SELF_RELATIONSHIP"
        )
        .otherwise("VALID")
        .alias("validation_status")
    )


# ============================================================
# 2. ENROLLMENT TIMELINE RECONSTRUCTION
# ============================================================

@dp.temporary_view(
    name="member_eligibility_timeline_source"
)
def member_eligibility_timeline_source():

    coverage = (
        spark.read.table(
            f"{CATALOG}.silver.coverage"
        )
        .filter(~F.col("is_deleted"))
        .alias("c")
    )

    enrollment = (
        spark.read.table(
            f"{CATALOG}.silver.enrollment"
        )
        .filter(~F.col("is_deleted"))
        .alias("e")
    )

    joined = (
        coverage.join(
            enrollment,
            F.col("c.coverage_id")
            == F.col("e.coverage_id"),
            "left"
        )
        .select(
            F.col("c.coverage_id"),
            F.col("c.member_id"),
            F.col("c.subscription_id"),
            F.col("c.benefit_plan_id"),
            F.col("c.coverage_start_date"),
            F.col("c.coverage_end_date"),
            F.col("c.coverage_revision"),

            F.col("e.enrollment_id"),
            F.col("e.member_id").alias(
                "enrollment_member_id"
            ),
            F.col("e.coverage_start_date").alias(
                "enrollment_coverage_start"
            ),
            F.col("e.coverage_end_date").alias(
                "enrollment_coverage_end"
            ),
            F.col("e.coverage_revision").alias(
                "enrollment_coverage_revision"
            ),
            F.col("e.enrollment_start_date"),
            F.col("e.enrollment_end_date"),
            F.col("e.tier_code"),
            F.col("e.pcp_practitioner_role_id"),
            F.col("e.change_reason_code"),
        )
    )

    order_window = (
        Window
        .partitionBy("coverage_id")
        .orderBy(
            "enrollment_start_date",
            "enrollment_end_date",
            "enrollment_id"
        )
    )

    coverage_window = (
        Window.partitionBy("coverage_id")
    )

    result = (
        joined
        .withColumn(
            "previous_end_date",
            F.lag(
                "enrollment_end_date"
            ).over(order_window)
        )
        .withColumn(
            "first_enrollment_start",
            F.min(
                "enrollment_start_date"
            ).over(coverage_window)
        )
        .withColumn(
            "last_enrollment_end",
            F.max(
                "enrollment_end_date"
            ).over(coverage_window)
        )
    )

    invalid_segment = (
        F.col("enrollment_start_date").isNull()
        | F.col("enrollment_end_date").isNull()
        | (
            F.col("enrollment_start_date")
            > F.col("enrollment_end_date")
        )
        | (
            F.col("enrollment_start_date")
            < F.col("coverage_start_date")
        )
        | (
            F.col("enrollment_end_date")
            > F.col("coverage_end_date")
        )
        | ~F.col("member_id").eqNullSafe(
            F.col("enrollment_member_id")
        )
        | ~F.col("coverage_start_date").eqNullSafe(
            F.col("enrollment_coverage_start")
        )
        | ~F.col("coverage_end_date").eqNullSafe(
            F.col("enrollment_coverage_end")
        )
        | ~F.col("coverage_revision").eqNullSafe(
            F.col("enrollment_coverage_revision")
        )
    )

    return result.withColumn(
        "segment_status",

        F.when(
            F.col("enrollment_id").isNull(),
            "MISSING_ENROLLMENT"
        )
        .when(
            invalid_segment,
            "INVALID_SEGMENT"
        )
        .when(
            F.col("previous_end_date").isNotNull()
            & (
                F.col("enrollment_start_date")
                <= F.col("previous_end_date")
            ),
            "OVERLAP"
        )
        .when(
            F.col("previous_end_date").isNotNull()
            & (
                F.col("enrollment_start_date")
                > F.date_add(
                    F.col("previous_end_date"),
                    1
                )
            ),
            "GAP"
        )
        .otherwise("VALID")
    )


# ============================================================
# 3. COVERAGE TIMELINE VALIDATION
# ============================================================

@dp.materialized_view(
    name=f"{CATALOG}.silver.coverage_timeline_validation",
    comment="Coverage timeline continuity and boundary checks",
)
def coverage_timeline_validation():

    timeline = spark.read.table(
        "member_eligibility_timeline_source"
    )

    return (
        timeline
        .groupBy(
            "coverage_id",
            "member_id",
            "coverage_start_date",
            "coverage_end_date"
        )
        .agg(
            F.count(
                "enrollment_id"
            ).alias("segment_count"),

            F.min(
                "first_enrollment_start"
            ).alias("first_start"),

            F.max(
                "last_enrollment_end"
            ).alias("last_end"),

            F.sum(
                F.when(
                    F.col("segment_status") != "VALID",
                    1
                ).otherwise(0)
            ).alias("invalid_segments")
        )
        .withColumn(
            "timeline_valid",

            (F.col("segment_count") > 0)

            & (F.col("invalid_segments") == 0)

            & F.col("first_start").eqNullSafe(
                F.col("coverage_start_date")
            )

            & F.col("last_end").eqNullSafe(
                F.col("coverage_end_date")
            )
        )
    )


# ============================================================
# 4. DERIVED MEMBER ELIGIBILITY
# ============================================================

@dp.materialized_view(
    name=f"{CATALOG}.silver.member_eligibility",
    comment="Conformed eligibility enriched with family and plan context",
)
def member_eligibility():

    # --------------------------------------------------------
    # 1. Accepted enrollment timeline
    # --------------------------------------------------------

    timeline = (
        spark.read.table(
            "member_eligibility_timeline_source"
        )
        .alias("t")
    )

    # --------------------------------------------------------
    # 2. Coverage timeline validation
    # --------------------------------------------------------

    coverage_valid = (
        spark.read.table(
            f"{CATALOG}.silver.coverage_timeline_validation"
        )
        .select(
            "coverage_id",
            "timeline_valid"
        )
        .alias("v")
    )

    # --------------------------------------------------------
    # 3. Current Subscription
    # --------------------------------------------------------

    subscription = (
        spark.read.table(
            f"{CATALOG}.silver.subscription"
        )
        .filter(
            F.col("__END_AT").isNull()
        )
        .filter(
            ~F.col("is_deleted")
        )
        .alias("s")
    )

    # --------------------------------------------------------
    # 4. Current family relationships
    # --------------------------------------------------------

    membership = (
        spark.read.table(
            f"{CATALOG}.silver.subscription_membership"
        )
        .filter(
            F.col("__END_AT").isNull()
        )
        .filter(
            ~F.col("is_deleted")
        )
        .alias("sm")
    )

    # --------------------------------------------------------
    # 5. Join timeline and Subscription
    # --------------------------------------------------------

    base = (
        timeline
        .join(
            coverage_valid,
            F.col("t.coverage_id")
            == F.col("v.coverage_id"),
            "left"
        )
        .join(
            subscription,
            F.col("t.subscription_id")
            == F.col("s.subscription_id"),
            "left"
        )
    )

    # --------------------------------------------------------
    # 6. Effective-dated membership relationship
    # --------------------------------------------------------

    relationship = (
        (
            F.col("t.subscription_id")
            == F.col("sm.subscription_id")
        )
        & (
            F.col("t.member_id")
            == F.col("sm.member_id")
        )
        & (
            F.col("sm.relationship_start_date")
            <= F.col("t.enrollment_start_date")
        )
        & (
            F.col("sm.relationship_end_date")
            >= F.col("t.enrollment_end_date")
        )
    )

    enriched = base.join(
        membership,
        relationship,
        "left"
    )

    # --------------------------------------------------------
    # 7. Select single relationship per Enrollment
    # --------------------------------------------------------

    relationship_window = (
        Window
        .partitionBy(
            "t.coverage_id",
            "t.enrollment_id"
        )
        .orderBy(
            F.col(
                "sm.relationship_start_date"
            ).desc_nulls_last(),

            F.col(
                "sm.subscription_member_id"
            ).asc_nulls_last()
        )
    )

    enriched = (
        enriched
        .withColumn(
            "_relationship_rank",
            F.row_number().over(
                relationship_window
            )
        )
        .filter(
            F.col("_relationship_rank") == 1
        )
    )

    # --------------------------------------------------------
    # 8. Eligibility validation
    # --------------------------------------------------------

    coverage_ok = (
        F.coalesce(
            F.col("v.timeline_valid"),
            F.lit(False)
        )
        & (
            F.col("t.segment_status") == "VALID"
        )
        & F.col(
            "s.subscription_id"
        ).isNotNull()
        & F.col(
            "sm.subscription_member_id"
        ).isNotNull()
        & (
            (
                F.upper(
                    F.col("sm.relationship_code")
                ) != "SELF"
            )
            | (
                F.col("t.member_id")
                == F.col("s.subscriber_member_id")
            )
        )
    )

    # --------------------------------------------------------
    # 9. Employer Group / Subgroup offering validation
    # --------------------------------------------------------

    offerings = (
        spark.read.table(f"{CATALOG}.silver.plan_offering")
        .filter(~F.col("is_deleted"))
        .filter(
            F.upper(F.trim(F.col("offering_status")))
            .isin("ACTIVE", "OFFERED")
        )
        .withColumn(
            "_scope",
            F.upper(F.trim(F.col("offering_scope")))
        )
    )

    segments = (
        enriched.select(
            F.struct(
                F.col("t.coverage_id"),
                F.col("t.enrollment_id")
            ).alias("_segment"),
            F.col("t.benefit_plan_id").alias("_plan"),
            F.col("t.enrollment_start_date").alias("_start"),
            F.col("t.enrollment_end_date").alias("_end"),
            F.col("s.employer_group_id").alias("_group"),
            F.col("s.employer_subgroup_id").alias("_subgroup"),
        )
        .dropDuplicates(["_segment"])
    )

    def offering_matches(scope):
        seg = segments.alias("seg")
        off = offerings.filter(
            F.col("_scope") == scope
        ).alias("off")

        condition = (
            (F.col("seg._group") == F.col("off.employer_group_id"))
            & (F.col("off.offering_start_date") <= F.col("seg._start"))
            & (F.col("off.offering_end_date") >= F.col("seg._end"))
        )

        if scope == "SUBGROUP":
            condition = (
                condition
                & (F.col("seg._subgroup")
                   == F.col("off.employer_subgroup_id"))
            )

        return (
            seg.join(off, condition, "inner")
            .groupBy("_segment")
            .agg(
                F.count("*").alias(f"_{scope.lower()}_count"),
                F.max(
                    F.when(
                        F.col("seg._plan")
                        == F.col("off.benefit_plan_id"),
                        1
                    ).otherwise(0)
                ).alias(f"_{scope.lower()}_valid")
            )
        )

    enriched = (
        enriched
        .withColumn(
            "_segment",
            F.struct(
                F.col("t.coverage_id"),
                F.col("t.enrollment_id")
            )
        )
        .join(offering_matches("SUBGROUP"), "_segment", "left")
        .join(offering_matches("GROUP"), "_segment", "left")
    )

    offering_valid = (
        F.when(
            F.col("s.coverage_market").isin(
                "INDIVIDUAL", "PUBLIC_PROGRAM"
            ),
            F.lit(True)
        )
        .when(
            F.coalesce(F.col("_subgroup_count"), F.lit(0)) > 0,
            F.col("_subgroup_valid") == 1
        )
        .otherwise(
            F.coalesce(F.col("_group_valid"), F.lit(0)) == 1
        )
    )


    # --------------------------------------------------------
    # 10. Final derived Silver output
    # --------------------------------------------------------

    return enriched.select(
        F.col("t.coverage_id").alias(
            "coverage_id"
        ),
        F.col("t.enrollment_id").alias(
            "enrollment_id"
        ),
        F.col("t.member_id").alias(
            "member_id"
        ),
        F.col("t.subscription_id").alias(
            "subscription_id"
        ),

        F.col(
            "s.subscriber_member_id"
        ).alias("subscriber_member_id"),

        F.col(
            "sm.relationship_code"
        ).alias("relationship_code"),

        F.col(
            "t.benefit_plan_id"
        ).alias("benefit_plan_id"),

        F.col(
            "t.coverage_start_date"
        ).alias("coverage_start_date"),

        F.col(
            "t.coverage_end_date"
        ).alias("coverage_end_date"),

        F.col(
            "t.enrollment_start_date"
        ).alias("enrollment_start_date"),

        F.col(
            "t.enrollment_end_date"
        ).alias("enrollment_end_date"),

        F.col("t.tier_code").alias(
            "tier_code"
        ),
        F.col(
            "t.pcp_practitioner_role_id"
        ).alias("pcp_practitioner_role_id"),

        coverage_ok.cast(
            "boolean"
        ).alias("coverage_valid_flag"),

        offering_valid.alias(
            "offering_valid_flag"
        ),
    )
