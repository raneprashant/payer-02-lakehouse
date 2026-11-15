
# ============================================================
# PAYER 360 - SILVER DERIVED
# DOMAIN 06: CLAIMS & FINANCE
#
# Derived Table 1: claim_financial
#
# Sources:
#   silver.claim_line
#   silver.claim_adjustment
#   silver.payment_remittance
#
# Grain: claim_id + line_number
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("source_catalog")

MONEY = "decimal(14,2)"


# ============================================================
# 1. CLAIM FINANCIAL
# ============================================================

@dp.materialized_view(
    name=f"{CATALOG}.silver.claim_financial",
    comment="Current claim-line financial reconciliation",
)
def claim_financial():

    # --------------------------------------------------------
    # 1. Current accepted Claim Lines (SCD1)
    # --------------------------------------------------------

    claim_lines = (
        spark.read.table(
            f"{CATALOG}.silver.claim_line"
        )
        .filter(~F.col("is_deleted"))
        .alias("cl")
    )

    # --------------------------------------------------------
    # 2. Aggregate signed Claim Adjustments
    #
    # All adjustments are retained as individual events.
    # Reversals are already signed by the source.
    # --------------------------------------------------------

    adjustments = (
        spark.read.table(
            f"{CATALOG}.silver.claim_adjustment"
        )
        .groupBy(
            "claim_id",
            "line_number"
        )
        .agg(
            F.sum("allowed_amount_delta").alias(
                "allowed_adjustment"
            ),
            F.sum("payer_liability_delta").alias(
                "payer_adjustment"
            )
        )
        .alias("adj")
    )

    # --------------------------------------------------------
    # 3. Aggregate Posted Payments
    #
    # Include only POSTED transactions.
    # Pending transactions do not reduce outstanding.
    #
    # ISSUE / VOID / RECOVERY amounts are signed.
    # We do not reverse their signs again.
    # --------------------------------------------------------

    payments = (
        spark.read.table(
            f"{CATALOG}.silver.payment_remittance"
        )
        .filter(
            F.upper(
                F.trim(F.col("payment_status"))
            ) == "POSTED"
        )
        .groupBy(
            "claim_id",
            "line_number"
        )
        .agg(
            F.sum("payment_amount").alias(
                "posted_payment"
            )
        )
        .alias("pay")
    )

    # --------------------------------------------------------
    # 4. Join all financial inputs
    # --------------------------------------------------------

    joined = (
        claim_lines
        .join(
            adjustments,
            (
                F.col("cl.claim_id")
                == F.col("adj.claim_id")
            )
            & (
                F.col("cl.line_number")
                == F.col("adj.line_number")
            ),
            "left"
        )
        .join(
            payments,
            (
                F.col("cl.claim_id")
                == F.col("pay.claim_id")
            )
            & (
                F.col("cl.line_number")
                == F.col("pay.line_number")
            ),
            "left"
        )
    )

    zero = F.lit(0).cast(MONEY)

    # --------------------------------------------------------
    # 5. Base financial values
    # --------------------------------------------------------

    result = joined.select(

        F.col("cl.claim_id").alias("claim_id"),

        F.col("cl.line_number").alias("line_number"),

        F.col("cl.billed_amount")
         .cast(MONEY)
         .alias("billed_amount"),

        F.col("cl.allowed_amount")
         .cast(MONEY)
         .alias("base_allowed_amount"),

        F.col("cl.payer_liability_amount")
         .cast(MONEY)
         .alias("base_payer_liability"),

        F.coalesce(
            F.col("adj.allowed_adjustment"),
            zero
        ).cast(MONEY).alias(
            "allowed_amount_adjustment_total"
        ),

        F.coalesce(
            F.col("adj.payer_adjustment"),
            zero
        ).cast(MONEY).alias(
            "payer_liability_adjustment_total"
        ),

        F.coalesce(
            F.col("pay.posted_payment"),
            zero
        ).cast(MONEY).alias(
            "net_posted_payment"
        ),
    )

    # --------------------------------------------------------
    # 6. Calculate Net Financial Balances
    # --------------------------------------------------------

    result = (
        result

        .withColumn(
            "net_allowed_amount",
            (
                F.col("base_allowed_amount")
                + F.col("allowed_amount_adjustment_total")
            ).cast(MONEY)
        )

        .withColumn(
            "net_payer_liability",
            (
                F.col("base_payer_liability")
                + F.col("payer_liability_adjustment_total")
            ).cast(MONEY)
        )

        .withColumn(
            "outstanding_amount",
            (
                F.col("net_payer_liability")
                - F.col("net_posted_payment")
            ).cast(MONEY)
        )
    )

    # --------------------------------------------------------
    # 7. Derive Payment Status
    #
    # CLOSED claim status does not imply PAID.
    # Payment status is derived from actual posted money.
    # --------------------------------------------------------

    result = result.withColumn(
        "derived_payment_status",

        F.when(
            F.col("net_payer_liability") <= 0,
            "NOT_PAYABLE"
        )
        .when(
            F.col("net_posted_payment") <= 0,
            "UNPAID"
        )
        .when(
            F.col("net_posted_payment")
            > F.col("net_payer_liability"),
            "OVERPAID"
        )
        .when(
            F.col("net_posted_payment")
            == F.col("net_payer_liability"),
            "PAID"
        )
        .otherwise("PARTIALLY_PAID")
    )

    # --------------------------------------------------------
    # 8. Financial Reconciliation
    # --------------------------------------------------------

    result = result.withColumn(
        "reconciliation_status",

        F.when(
            F.col("net_allowed_amount").isNull()
            | F.col("net_payer_liability").isNull(),
            "MISSING_FINANCIAL_AMOUNT"
        )
        .when(
            F.col("net_allowed_amount") < 0,
            "INVALID_ALLOWED_AMOUNT"
        )
        .when(
            F.col("net_payer_liability") < 0,
            "INVALID_PAYER_LIABILITY"
        )
        .when(
            F.col("net_posted_payment") < 0,
            "NEGATIVE_PAYMENT_BALANCE"
        )
        .when(
            F.col("net_payer_liability")
            > F.col("net_allowed_amount"),
            "LIABILITY_EXCEEDS_ALLOWED"
        )
        .when(
            F.col("net_posted_payment")
            > F.col("net_payer_liability"),
            "OVERPAYMENT_REVIEW"
        )
        .otherwise("RECONCILED")
    )

    # --------------------------------------------------------
    # 9. Return approved Blueprint columns
    # --------------------------------------------------------

    return result.select(
        "claim_id",
        "line_number",
        "billed_amount",
        "base_allowed_amount",
        "base_payer_liability",
        "allowed_amount_adjustment_total",
        "payer_liability_adjustment_total",
        "net_allowed_amount",
        "net_payer_liability",
        "net_posted_payment",
        "outstanding_amount",
        "derived_payment_status",
        "reconciliation_status",
    )

# ============================================================
# 2. CLAIM ENRICHED
# Effective-dated claim resolution
# ============================================================

from pyspark.sql.window import Window


def service_date_in(date_col, start_col, end_col):
    """
    Inclusive source business-effective period.
    """
    return (
        (date_col >= F.col(start_col))
        & (
            date_col <= F.coalesce(
                F.col(end_col),
                F.lit("2999-12-31").cast("date")
            )
        )
    )


def latest_scd2(base, entity, match, output_columns):
    """
    Resolve the latest applicable SCD2 source version
    for each claim line.

    Business-effective dates are applied by match.

    Do not filter __END_AT IS NULL here, because
    historical service dates may require older versions.
    """

    dimension = (
        spark.read.table(
            f"{CATALOG}.silver.{entity}"
        )
        .filter(~F.col("is_deleted"))
        .alias("d")
    )

    joined = (
        base.alias("b")
        .join(dimension, match, "left")
    )

    window = (
        Window
        .partitionBy(
            F.col("b.claim_id"),
            F.col("b.line_number")
        )
        .orderBy(
            F.col(
                "d._source_record_version"
            ).desc_nulls_last(),
            F.col(
                "d._source_sequence"
            ).desc_nulls_last()
        )
    )

    return (
        joined
        .withColumn(
            "_scd_rank",
            F.row_number().over(window)
        )
        .filter(F.col("_scd_rank") == 1)
        .select(
            "b.*",
            *[
                F.col(f"d.{source}").alias(target)
                for source, target in output_columns.items()
            ]
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.claim_enriched",
    comment=(
        "Service-date eligibility, provider, network "
        "and contract resolution"
    ),
)
def claim_enriched():

    # ========================================================
    # A. CLAIM LINE + CLAIM HEADER
    # ========================================================

    line = (
        spark.read.table(
            f"{CATALOG}.silver.claim_line"
        )
        .filter(~F.col("is_deleted"))
        .alias("l")
    )

    header = (
        spark.read.table(
            f"{CATALOG}.silver.claim_header"
        )
        .filter(~F.col("is_deleted"))
        .alias("h")
    )

    base = (
        line
        .join(
            header,
            F.col("l.claim_id") == F.col("h.claim_id"),
            "left"
        )
        .select(
            F.col("l.claim_id").alias("claim_id"),
            F.col("l.line_number").alias("line_number"),
            F.col("h.member_id").alias("member_id"),
            F.col("l.service_date").alias("service_date"),
            F.col("l.rendering_practitioner_role_id"),
            F.col("l.procedure_code"),
            F.col("l.modifier_code")
        )
    )

    # ========================================================
    # B. SERVICE-DATE MEMBER ELIGIBILITY
    # ========================================================

    eligibility = (
        spark.read.table(
            f"{CATALOG}.silver.member_eligibility"
        )
        .alias("e")
    )

    ej = (
        base.alias("b")
        .join(
            eligibility,
            (
                F.col("b.member_id")
                == F.col("e.member_id")
            )
            & service_date_in(
                F.col("b.service_date"),
                "e.enrollment_start_date",
                "e.enrollment_end_date"
            ),
            "left"
        )
    )

    claim_window = (
        Window.partitionBy(
            F.col("b.claim_id"),
            F.col("b.line_number")
        )
    )

    eligibility_rank = claim_window.orderBy(
        F.col(
            "e.coverage_valid_flag"
        ).desc_nulls_last(),
        F.col(
            "e.enrollment_start_date"
        ).desc_nulls_last(),
        F.col("e.coverage_id").asc_nulls_last(),
        F.col("e.enrollment_id").asc_nulls_last()
    )

    enriched = (
        ej
        .withColumn(
            "_eligibility_matches",
            F.count(
                F.col("e.enrollment_id")
            ).over(claim_window)
        )
        .withColumn(
            "_elig_rank",
            F.row_number().over(eligibility_rank)
        )
        .filter(F.col("_elig_rank") == 1)
        .select(
            "b.*",
            "_eligibility_matches",
            F.col("e.coverage_id").alias("coverage_id"),
            F.col("e.enrollment_id").alias("enrollment_id"),
            F.col("e.benefit_plan_id").alias(
                "benefit_plan_id"
            ),
            F.col("e.subscription_id").alias(
                "subscription_id"
            ),
            F.col("e.coverage_valid_flag").alias(
                "eligibility_valid_flag"
            ),
            F.col("e.offering_valid_flag").alias(
                "_offering_valid_flag"
            )
        )
    )

    # ========================================================
    # C. PROVIDER AFFILIATION
    # ========================================================

    role = latest_scd2(
        enriched,
        "practitioner_role",
        (
            F.col("b.rendering_practitioner_role_id")
            == F.col("d.practitioner_role_id")
        )
        & service_date_in(
            F.col("b.service_date"),
            "d.effective_start_date",
            "d.effective_end_date"
        ),
        {
            "practitioner_role_id": "_matched_role_id",
            "practitioner_id": "_role_practitioner_id",
            "supplier_location_id": "_role_location_id",
            "specialty_id": "specialty_id"
        }
    )

    practitioner = latest_scd2(
        role,
        "practitioner",
        (
            F.col("b._role_practitioner_id")
            == F.col("d.practitioner_id")
        )
        & service_date_in(
            F.col("b.service_date"),
            "d.effective_start_date",
            "d.effective_end_date"
        ),
        {
            "practitioner_id": "practitioner_id"
        }
    )

    location = latest_scd2(
        practitioner,
        "supplier_location",
        (
            F.col("b._role_location_id")
            == F.col("d.supplier_location_id")
        )
        & service_date_in(
            F.col("b.service_date"),
            "d.effective_start_date",
            "d.effective_end_date"
        ),
        {
            "supplier_location_id": "supplier_location_id",
            "supplier_id": "_location_supplier_id"
        }
    )

    provider = latest_scd2(
        location,
        "supplier",
        (
            F.col("b._location_supplier_id")
            == F.col("d.supplier_id")
        )
        & service_date_in(
            F.col("b.service_date"),
            "d.effective_start_date",
            "d.effective_end_date"
        ),
        {
            "supplier_id": "supplier_id"
        }
    )

    # ========================================================
    # D. PLAN NETWORK
    # ========================================================

    pn = (
        spark.read.table(
            f"{CATALOG}.silver.plan_network"
        )
        .filter(~F.col("is_deleted"))
        .alias("pn")
    )

    n = (
        spark.read.table(
            f"{CATALOG}.silver.network"
        )
        .filter(~F.col("is_deleted"))
        .alias("n")
    )

    nw = (
        provider.alias("b")
        .join(
            pn,
            (
                F.col("b.benefit_plan_id")
                == F.col("pn.benefit_plan_id")
            )
            & service_date_in(
                F.col("b.service_date"),
                "pn.effective_start_date",
                "pn.effective_end_date"
            ),
            "left"
        )
        .join(
            n,
            (
                F.col("pn.network_id")
                == F.col("n.network_id")
            )
            & service_date_in(
                F.col("b.service_date"),
                "n.effective_start_date",
                "n.effective_end_date"
            ),
            "left"
        )
    )

    network_window = (
        Window
        .partitionBy(
            F.col("b.claim_id"),
            F.col("b.line_number"),
            F.col("pn.plan_network_id")
        )
        .orderBy(
            F.col(
                "n._source_record_version"
            ).desc_nulls_last(),
            F.col(
                "n._source_sequence"
            ).desc_nulls_last()
        )
    )

    networks = (
        nw
        .withColumn(
            "_network_rank",
            F.row_number().over(network_window)
        )
        .filter(F.col("_network_rank") == 1)
        .select(
            "b.*",
            F.col("pn.plan_network_id").alias(
                "_plan_network_id"
            ),
            F.col("n.network_id").alias("_network_id")
        )
    )

    # ========================================================
    # E. NETWORK PARTICIPATION
    #
    # Match precedence:
    #   Practitioner Role = 3
    #   Supplier Location = 2
    #   Supplier = 1
    # ========================================================

    np = (
        spark.read.table(
            f"{CATALOG}.silver.network_participation"
        )
        .filter(~F.col("is_deleted"))
        .alias("np")
    )

    kind = F.upper(
        F.trim(F.col("np.participant_entity_type"))
    )

    participation_scope = (
        F.when(
            (kind == "PRACTITIONER_ROLE")
            & (
                F.col("np.participant_entity_id")
                == F.col("b.rendering_practitioner_role_id")
            ),
            3
        )
        .when(
            (kind == "SUPPLIER_LOCATION")
            & (
                F.col("np.participant_entity_id")
                == F.col("b.supplier_location_id")
            ),
            2
        )
        .when(
            (kind == "SUPPLIER")
            & (
                F.col("np.participant_entity_id")
                == F.col("b.supplier_id")
            ),
            1
        )
        .otherwise(0)
    )

    participant_match = (
        (
            F.col("b._network_id")
            == F.col("np.network_id")
        )
        & service_date_in(
            F.col("b.service_date"),
            "np.effective_start_date",
            "np.effective_end_date"
        )
        & (
            F.col("np.supplier_location_id").isNull()
            | (
                F.col("np.supplier_location_id")
                == F.col("b.supplier_location_id")
            )
        )
        & (participation_scope > 0)
    )

    participating = (
        networks.alias("b")
        .join(
            np,
            participant_match,
            "left"
        )
        .select(
            "b.*",
            F.col("np.participation_id").alias(
                "_participation_id"
            ),
            F.col("np.participation_status").alias(
                "participation_status"
            ),
            participation_scope.alias(
                "_participation_scope"
            )
        )
    )

    # ========================================================
    # F. PROVIDER CONTRACT
    # ========================================================

    ct = (
        spark.read.table(
            f"{CATALOG}.silver.provider_contract"
        )
        .filter(~F.col("is_deleted"))
        .alias("ct")
    )

    ctype = F.upper(
        F.trim(F.col("ct.contract_entity_type"))
    )

    contract_scope = (
        F.when(
            (ctype == "PRACTITIONER_ROLE")
            & (
                F.col("ct.contract_entity_id")
                == F.col("b.rendering_practitioner_role_id")
            ),
            3
        )
        .when(
            (ctype == "SUPPLIER_LOCATION")
            & (
                F.col("ct.contract_entity_id")
                == F.col("b.supplier_location_id")
            ),
            2
        )
        .when(
            (ctype == "SUPPLIER")
            & (
                F.col("ct.contract_entity_id")
                == F.col("b.supplier_id")
            ),
            1
        )
        .otherwise(0)
    )

    contracted = (
        participating.alias("b")
        .join(
            ct,
            (
                F.col("b._network_id")
                == F.col("ct.network_id")
            )
            & (contract_scope > 0)
            & service_date_in(
                F.col("b.service_date"),
                "ct.effective_start_date",
                "ct.effective_end_date"
            ),
            "left"
        )
        .select(
            "b.*",
            F.col("ct.contract_id").alias("contract_id"),
            contract_scope.alias("_contract_scope")
        )
    )

    # ========================================================
    # G. PROVIDER CONTRACT RATE
    #
    # Match:
    #  - Contract
    #  - Benefit Plan
    #  - Procedure
    #  - Modifier (exact or generic)
    #  - Effective service date
    #
    # Our Claim Line source has no place_of_service_code.
    # Only generic (blank POS) rates are safe to match.
    # ========================================================

    rt = (
        spark.read.table(
            f"{CATALOG}.silver.provider_contract_rate"
        )
        .filter(~F.col("is_deleted"))
        .alias("rt")
    )

    rate_modifier = F.upper(
        F.trim(
            F.coalesce(
                F.col("rt.modifier_code"),
                F.lit("")
            )
        )
    )

    claim_modifier = F.upper(
        F.trim(
            F.coalesce(
                F.col("b.modifier_code"),
                F.lit("")
            )
        )
    )

    rate_pos = F.trim(
        F.coalesce(
            F.col("rt.place_of_service_code"),
            F.lit("")
        )
    )

    rates = (
        contracted.alias("b")
        .join(
            rt,
            (
                F.col("b.contract_id")
                == F.col("rt.contract_id")
            )
            & (
                F.col("b.benefit_plan_id")
                == F.col("rt.benefit_plan_id")
            )
            & (
                F.col("b.procedure_code")
                == F.col("rt.procedure_code")
            )
            & (
                (rate_modifier == claim_modifier)
                | (rate_modifier == "")
            )
            #& (rate_pos == "")
            & service_date_in(
                F.col("b.service_date"),
                "rt.effective_start_date",
                "rt.effective_end_date"
            ),
            "left"
        )
        .select(
            "b.*",
            (rate_pos != "").alias("_pos_specific"),
            F.col("rt.contract_rate_id").alias(
                "contract_rate_id"
            ),
            F.col("rt.negotiated_rate")
             .cast("decimal(14,2)")
             .alias("negotiated_rate"),

            F.when(
                F.col("rt.contract_rate_id").isNotNull()
                & (rate_modifier != ""),
                1
            )
            .otherwise(0)
            .alias("_specific_modifier")
        )
    )

    # ========================================================
    # H. CANDIDATE SELECTION AND AMBIGUITY DETECTION
    # ========================================================

    complete = (
        F.col("_network_id").isNotNull()
        & F.col("_participation_id").isNotNull()
        & F.col("contract_id").isNotNull()
        & F.col("contract_rate_id").isNotNull()
    )

    quality_score = (
        F.when(complete, 10000).otherwise(0)
        + F.when(
            F.col("contract_rate_id").isNotNull(),
            1000
        ).otherwise(0)
        + F.when(
            F.col("_participation_id").isNotNull(),
            100
        ).otherwise(0)
        + F.coalesce(
            F.col("_participation_scope"),
            F.lit(0)
        ) * 10
        + F.coalesce(
            F.col("_contract_scope"),
            F.lit(0)
        )
        + F.col("_specific_modifier") * 5
    )

    scored = rates.withColumn(
        "_quality_score",
        quality_score
    )

    w = Window.partitionBy(
        "claim_id",
        "line_number"
    )

    scored = (
        scored
        .withColumn(
            "_best_quality",
            F.max("_quality_score").over(w)
        )
        .withColumn(
            "_top_path",
            F.when(
                complete
                & (
                    F.col("_quality_score")
                    == F.col("_best_quality")
                ),
                F.struct(
                    "_network_id",
                    "_participation_id",
                    "contract_id",
                    "contract_rate_id"
                )
            )
        )
    )

    top_path_counts = (
        scored
        .groupBy("claim_id", "line_number")
        .agg(
            F.countDistinct("_top_path").alias(
                "_best_path_count"
            )
        )
    )

    rank_window = w.orderBy(
        F.col("_quality_score").desc(),
        F.col("_network_id").asc_nulls_last(),
        F.col("_participation_id").asc_nulls_last(),
        F.col("contract_id").asc_nulls_last(),
        F.col("contract_rate_id").asc_nulls_last()
    )

    chosen = (
        scored
        .join(
            top_path_counts,
            ["claim_id", "line_number"],
            "left"
        )
        .withColumn(
            "_selected_rank",
            F.row_number().over(rank_window)
        )
        .filter(F.col("_selected_rank") == 1)
    )

    # ========================================================
    # I. FINAL APPROVED SILVER OUTPUT
    # ========================================================

    return chosen.select(
        "claim_id",
        "line_number",
        "member_id",
        "service_date",
        "coverage_id",
        "enrollment_id",
        "benefit_plan_id",
        "subscription_id",
        "rendering_practitioner_role_id",
        "practitioner_id",
        "supplier_location_id",
        "supplier_id",
        "specialty_id",

        F.col("_network_id").alias("network_id"),

        "participation_status",
        "contract_id",
        "contract_rate_id",
        "negotiated_rate",
        "eligibility_valid_flag",

        F.when(
            F.col("member_id").isNull(),
            "MISSING_CLAIM_HEADER"
        )
        .when(
            F.col("service_date").isNull(),
            "MISSING_SERVICE_DATE"
        )
        .when(
            F.col("_eligibility_matches") > 1,
            "AMBIGUOUS_ELIGIBILITY"
        )
        .when(
            F.col("coverage_id").isNull(),
            "MISSING_ELIGIBILITY"
        )
        .when(
            ~F.coalesce(
                F.col("eligibility_valid_flag"),
                F.lit(False)
            ),
            "INVALID_ELIGIBILITY"
        )
        .when(
            F.col("_offering_valid_flag").isNull(),
            "OFFERING_NOT_VALIDATED"
        )
        .when(
            ~F.col("_offering_valid_flag"),
            "INVALID_PLAN_OFFERING"
        )
        .when(
            F.col("_matched_role_id").isNull(),
            "MISSING_PROVIDER_ROLE"
        )
        .when(
            F.col("practitioner_id").isNull(),
            "MISSING_PRACTITIONER"
        )
        .when(
            F.col("supplier_location_id").isNull(),
            "MISSING_SUPPLIER_LOCATION"
        )
        .when(
            F.col("supplier_id").isNull(),
            "MISSING_SUPPLIER"
        )
        .when(
            F.col("_best_path_count") > 1,
            "AMBIGUOUS_NETWORK_CONTRACT_RATE"
        )
        .when(
            F.col("_plan_network_id").isNull(),
            "MISSING_PLAN_NETWORK"
        )
        .when(
            F.col("_network_id").isNull(),
            "MISSING_NETWORK"
        )
        .when(
            F.col("_participation_id").isNull(),
            "MISSING_NETWORK_PARTICIPATION"
        )
        .when(
            F.col("contract_id").isNull(),
            "MISSING_PROVIDER_CONTRACT"
        )
        .when(
            F.col("contract_rate_id").isNull(),
            "MISSING_CONTRACT_RATE"
        )
        .when(
            F.col("_pos_specific"),
            "POS_NOT_VERIFIED"
        )
        .otherwise("RESOLVED")
        .alias("resolution_status")
    )
