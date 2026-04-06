# =============================================================================
# Lakeflow Declarative Pipeline  (DLT / Python)
# Pipeline name : electroniz_gold_mdm_pipeline
# Catalog       : electroniz_catalog
# Schema        : electroniz_gold_master_schema
#
# Sources
#   electroniz_catalog.silver.electroniz_silver_customers
#   electroniz_catalog.silver.electroniz_silver_ecomm_transactions
#
# Targets
#   electroniz_catalog.gold.mvw_electroniz_gold_golden_record_customers
#   electroniz_catalog.gold.mvw_electroniz_gold_linking_record_customers
# =============================================================================

import dlt
import re
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, LongType

# ---------------------------------------------------------------------------
# 0.  CATALOG / SCHEMA CONSTANTS
# ---------------------------------------------------------------------------
CATALOG = "electroniz_catalog"
SOURCE_SCHEMA = "electroniz_silver_schema"
SILVER_CUSTOMER_TABLE = "electroniz_silver_customers"
SILVER_ECOMM_TABLE = "electroniz_silver_ecomm_transactions"
TARGET_SCHEMA = "electroniz_gold_master_schema"

# ---------------------------------------------------------------------------
# 1.  HELPER UDFs  (registered once, reused across the pipeline)
# ---------------------------------------------------------------------------

# --- 1a. Name normalisation  ------------------------------------------------
#  Lower-case, collapse whitespace, strip punctuation → used for fuzzy match
@F.udf(StringType())
def normalize_name(name: str) -> str:
    if not name:
        return None
    n = name.lower()
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


# --- 1b. Address normalisation  --------------------------------------------
#  Expand abbreviations (whole-word, case-insensitive), collapse whitespace
ABBREV = {
    r"\bst\.?\b":  "Street",
    r"\bav\.?\b":  "Avenue",
    r"\bdr\.?\b":  "Drive",
    r"\brd\.?\b":  "Road",
    r"\bln\.?\b":  "Lane",
    r"\bap\.?\b":  "Apartment",
}

@F.udf(StringType())
def expand_address(addr: str) -> str:
    if not addr:
        return None
    a = addr.strip()
    for pattern, replacement in ABBREV.items():
        a = re.sub(pattern, replacement, a, flags=re.IGNORECASE)
    # Remove hash/pound sign
    a = a.replace("#", " ")
    a = re.sub(r"\s+", " ", a).strip()
    return a


@F.udf(StringType())
def normalize_address_for_match(addr: str) -> str:
    """Normalise address for blocking / similarity comparison."""
    if not addr:
        return None
    a = addr.strip()
    for pattern, replacement in ABBREV.items():
        a = re.sub(pattern, replacement, a, flags=re.IGNORECASE)
    a = a.lower()
    a = re.sub(r"[^a-z0-9\s]", " ", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a


# --- 1c. Phone formatting and quality scoring  ------------------------------
@F.udf(LongType())
def phone_format_score(phone: str) -> int:
    """
    Return phone formatting quality score based on country detection and format:
    5 = Perfect country-specific format (e.g., +1 (XXX) XXX-XXXX, +44 XXXX XXXXXX, +91 XXXXX XXXXX)
    4 = Has country code prefix (+1, +44, +91, etc.)
    3 = Has formatting chars but no country code
    2 = Has some formatting (parentheses, hyphens, spaces)
    1 = Just digits (unformatted)
    0 = Invalid (< 7 digits or null)
    """
    if not phone:
        return 0
    
    phone_stripped = phone.strip()
    digits = re.sub(r"\D", "", phone)
    
    # Invalid: less than 7 digits
    if len(digits) < 7:
        return 0
    
    # Count formatting characters (+, parentheses, hyphens, spaces)
    format_chars = len(re.findall(r"[+()\-\s]", phone))
    has_plus = phone_stripped.startswith("+")
    
    # US/Canada: 10 digits or 11 digits starting with 1
    if re.match(r"^1?\d{10}$", digits):
        if len(digits) == 11 and digits[0] != "1":
            pass  # Not US/Canada pattern
        elif len(digits) == 10 and digits[0] not in "23456789":
            pass  # Not US/Canada pattern
        else:
            # Valid US/Canada number
            # Check for perfect format: +1 (XXX) XXX-XXXX
            if re.match(r"^\+1\s?\(\d{3}\)\s?\d{3}-\d{4}$", phone_stripped):
                return 5
            # Has +1 prefix
            if has_plus and "1" in phone_stripped[:3]:
                return 4
            # Has formatting but no country code
            if format_chars >= 2:
                return 3
            if format_chars >= 1:
                return 2
            # Just digits
            return 1
    
    # UK: 12 digits starting with 44, or 11 digits starting with 0
    if re.match(r"^44\d{10}$", digits) or re.match(r"^0\d{10}$", digits):
        # Check for perfect format: +44 XXXX XXXXXX
        if re.match(r"^\+44\s\d{4}\s\d{6}$", phone_stripped):
            return 5
        # Has +44 prefix
        if has_plus and "44" in phone_stripped[:4]:
            return 4
        # Has formatting but no country code
        if format_chars >= 2:
            return 3
        if format_chars >= 1:
            return 2
        # Just digits
        return 1
    
    # India: 12 digits starting with 91, or 10 digits starting with [6-9]
    if re.match(r"^91\d{10}$", digits) or re.match(r"^[6-9]\d{9}$", digits):
        # Check for perfect format: +91 XXXXX XXXXX
        if re.match(r"^\+91\s\d{5}\s\d{5}$", phone_stripped):
            return 5
        # Has +91 prefix
        if has_plus and "91" in phone_stripped[:4]:
            return 4
        # Has formatting but no country code
        if format_chars >= 2:
            return 3
        if format_chars >= 1:
            return 2
        # Just digits
        return 1
    
    # Unknown country (>= 7 digits, doesn't match above patterns)
    if len(digits) >= 7:
        # Has + prefix (generic international format)
        if has_plus:
            return 4
        # Has formatting
        if format_chars >= 2:
            return 3
        if format_chars >= 1:
            return 2
        # Just digits
        return 1
    
    return 0


@F.udf(StringType())
def normalize_phone_for_match(phone: str) -> str:
    """Normalize phone to digits only for matching."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    # Return last 10 digits for US/Canada, or all digits if less
    return digits[-10:] if len(digits) >= 10 else digits


@F.udf(LongType())
def digit_count(phone: str) -> int:
    if not phone:
        return None
    return len(re.sub(r"\D", "", phone))


# --- 1d. Email domain rank  -------------------------------------------------
@F.udf(LongType())
def email_rank(email: str) -> int:
    if not email:
        return 99
    domain = email.strip().lower().split("@")[-1]
    ranks = {"gmail.com": 1, "hotmail.com": 2, "yahoo.com": 3}
    return ranks.get(domain, 4)


@F.udf(StringType())
def normalize_email_for_match(email: str) -> str:
    """Normalize email for matching (lowercase, strip)."""
    if not email:
        return None
    return email.strip().lower()


# --- 1e. Name parsing (prefix / suffix extraction)  ------------------------
PREFIXES = {"mr", "mrs", "ms", "miss", "dr", "prof", "rev", "sir"}
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "md", "esq"}

@F.udf(StringType())
def resolve_full_name(name: str) -> str:
    """Return ONLY the core name with prefix/suffix tokens removed (for blocking/matching)."""
    if not name:
        return None
    parts = name.strip().split()
    filtered = [
        p for p in parts
        if p.lower().rstrip(".") not in PREFIXES
        and p.lower().rstrip(".") not in SUFFIXES
    ]
    resolved = " ".join(filtered)
    return resolved.strip() if resolved.strip() else name


# ---------------------------------------------------------------------------
# 2.  INGEST SOURCE TABLES  (read-only streaming views)
# ---------------------------------------------------------------------------

@dlt.view(
    name="vw_mdm_customers",
    comment="Streaming read of electroniz_silver_customers",
)
def vw_mdm_customers():
    return (
        spark.read.format("delta")
        .table(f"{CATALOG}.{SOURCE_SCHEMA}.{SILVER_CUSTOMER_TABLE}")
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("customer_name").cast("string"),
            F.col("email").cast("string"),
            F.col("phone").cast("string"),
            F.col("address").cast("string"),
            F.col("postalcode").cast("string").alias("postal_code"),
            F.col("credit_card").cast("string"),
            F.col("updated_at").cast("timestamp"),
            F.lit("customers").alias("_source"),
        )
    )


@dlt.view(
    name="vw_mdm_ecommerce",
    comment="Streaming read of electroniz_silver_ecommerce_transactions",
)
def vw_mdm_ecommerce():
    return (
        spark.read.format("delta")
        .table(f"{CATALOG}.{SOURCE_SCHEMA}.{SILVER_ECOMM_TABLE}")
        .select(
            F.col("ecomm_customer_id").cast("string").alias("ecomm_customer_id"),
            F.col("customer_name").cast("string"),
            F.col("email").cast("string"),
            F.col("phone").cast("string"),
            F.col("address").cast("string"),
            F.col("postalcode").cast("string").alias("postal_code"),
            F.lit(None).cast("string").alias("credit_card"),  # Ecommerce table has no credit card
            F.col("_ingested_at").cast("timestamp").alias("updated_at"),
            F.lit("ecommerce").alias("_source"),
        )
    )


# ---------------------------------------------------------------------------
# 3.  BLOCKING & FUZZY MATCHING → CLUSTER ASSIGNMENT
#
#  Strategy (free-edition friendly — pure Spark, no ML libs):
#   • Normalise customer_name, address, phone, email into blocking/matching keys.
#   • Block on first-token of name OR first-5-chars of address
#     → Cartesian is bounded within each block.
#   • Within each block, match if any of:
#     - Name trigram similarity ≥ 0.55 AND address trigram similarity ≥ 0.40
#     - Exact phone match (normalized)
#     - Exact email match (normalized)
#   • Assign cluster_id via connected-component labelling using
#     iterative self-join (converges in 2-3 passes for typical data).
# ---------------------------------------------------------------------------

def _trigrams(s: str):
    """Return set of character trigrams for a string."""
    if not s or len(s) < 3:
        return set(s) if s else set()
    return {s[i:i+3] for i in range(len(s) - 2)}

@F.udf(StringType())
def name_block_key(name: str) -> str:
    """First whitespace-token of normalised name (blocking key)."""
    if not name:
        return "__null__"
    n = re.sub(r"[^a-z0-9\s]", " ", name.lower()).strip()
    tokens = n.split()
    return tokens[0] if tokens else "__null__"

@F.udf(StringType())
def addr_block_key(addr: str) -> str:
    """First 5 chars of normalised address (blocking key)."""
    if not addr:
        return "__null__"
    a = re.sub(r"[^a-z0-9]", "", addr.lower())
    return a[:5] if len(a) >= 5 else a

@F.udf("double")
def jaccard_trigrams(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    ta = _trigrams(a)
    tb = _trigrams(b)
    inter = len(ta & tb)
    union = len(ta | tb)
    return float(inter) / float(union) if union else 0.0


# ---------------------------------------------------------------------------
# 3a.  Combined staging table (customers ∪ ecommerce)
# ---------------------------------------------------------------------------
@dlt.table(
    name="mvw_mdm_combined_staging",
    comment="Union of customers and ecommerce records with normalised keys",
    table_properties={"pipelines.reset.allowed": "true"},
)
def mvw_mdm_combined_staging():
    cust = dlt.read("vw_mdm_customers").withColumn(
        "record_id", F.concat(F.lit("C_"), F.col("customer_id"))
    )
    ecomm = dlt.read("vw_mdm_ecommerce").withColumn(
        "record_id", F.concat(F.lit("E_"), F.col("ecomm_customer_id"))
    ).withColumnRenamed("ecomm_customer_id", "customer_id")

    combined = cust.unionByName(ecomm, allowMissingColumns=True)

    return (
        combined
        .withColumn("resolved_name", resolve_full_name(F.col("customer_name")))  # Strip prefixes/suffixes for blocking/matching
        .withColumn("norm_name",     normalize_name(F.col("resolved_name")))     # Normalize the stripped name
        .withColumn("norm_address", normalize_address_for_match(F.col("address")))
        .withColumn("norm_phone",   normalize_phone_for_match(F.col("phone")))
        .withColumn("norm_email",   normalize_email_for_match(F.col("email")))
        .withColumn("name_bk",      name_block_key(F.col("norm_name")))
        .withColumn("addr_bk",      addr_block_key(F.col("norm_address")))
        # initialise cluster_id = record_id (each record is its own cluster)
        .withColumn("cluster_id",   F.col("record_id"))
    )


# ---------------------------------------------------------------------------
# 3b.  Match pairs within blocks
# ---------------------------------------------------------------------------
@dlt.table(
    name="mvw_mdm_match_pairs",
    comment="Pairs of records that match on name+address similarity OR exact phone/email match",
    table_properties={"pipelines.reset.allowed": "true"},
)
def mvw_mdm_match_pairs():
    staging = dlt.read("mvw_mdm_combined_staging")

    # Self-join on blocking keys
    left  = staging.alias("l")
    right = staging.alias("r")

    pairs = left.join(
        right,
        on=(
            (F.col("l.name_bk") == F.col("r.name_bk")) |
            (F.col("l.addr_bk") == F.col("r.addr_bk"))
        )
    ).filter(
        F.col("l.record_id") < F.col("r.record_id")      # avoid duplicates
    ).select(
        F.col("l.record_id").alias("record_id_a"),
        F.col("r.record_id").alias("record_id_b"),
        F.col("l.norm_name").alias("name_a"),
        F.col("r.norm_name").alias("name_b"),
        F.col("l.norm_address").alias("addr_a"),
        F.col("r.norm_address").alias("addr_b"),
        F.col("l.norm_phone").alias("phone_a"),
        F.col("r.norm_phone").alias("phone_b"),
        F.col("l.norm_email").alias("email_a"),
        F.col("r.norm_email").alias("email_b"),
    )

    # Compute similarities and exact matches
    pairs_with_scores = (
        pairs
        .withColumn("name_sim", jaccard_trigrams(F.col("name_a"), F.col("name_b")))
        .withColumn("addr_sim", jaccard_trigrams(F.col("addr_a"), F.col("addr_b")))
        .withColumn(
            "phone_match",
            F.when(
                (F.col("phone_a").isNotNull()) & (F.col("phone_b").isNotNull()) & 
                (F.col("phone_a") == F.col("phone_b")),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "email_match",
            F.when(
                (F.col("email_a").isNotNull()) & (F.col("email_b").isNotNull()) & 
                (F.col("email_a") == F.col("email_b")),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )

    # Match if ANY of these conditions are true:
    # 1. High name AND address similarity (fuzzy match)
    # 2. Exact phone match
    # 3. Exact email match
    return (
        pairs_with_scores
        .filter(
            ((F.col("name_sim") >= 0.55) & (F.col("addr_sim") >= 0.40)) |
            F.col("phone_match") |
            F.col("email_match")
        )
        .select(
            "record_id_a", 
            "record_id_b", 
            "name_sim", 
            "addr_sim",
            "phone_match",
            "email_match"
        )
    )


# ---------------------------------------------------------------------------
# 3c.  Connected-component cluster resolution (2 iterations)
#      Iteration 0 : propagate min(record_id) within each match edge
#      Iteration 1 : propagate again to handle transitive chains
# ---------------------------------------------------------------------------
@dlt.table(
    name="mvw_mdm_clusters",
    comment="Cluster assignments after connected-component resolution",
    table_properties={"pipelines.reset.allowed": "true"},
)
def mvw_mdm_clusters():
    staging = dlt.read("mvw_mdm_combined_staging").select("record_id", "cluster_id")
    pairs   = dlt.read("mvw_mdm_match_pairs")

    # Build edge list  (both directions)
    edges = (
        pairs.select(F.col("record_id_a").alias("src"), F.col("record_id_b").alias("dst"))
        .union(
            pairs.select(F.col("record_id_b").alias("src"), F.col("record_id_a").alias("dst"))
        )
    )

    # Pass 1 – assign min neighbour as cluster
    pass1 = (
        staging.alias("s")
        .join(edges.alias("e"), F.col("s.record_id") == F.col("e.src"), "left")
        .groupBy("s.record_id")
        .agg(
            F.min(
                F.coalesce(F.col("e.dst"), F.col("s.record_id"))
            ).alias("cluster_id_p1")
        )
        .select(
            F.col("record_id"),
            F.least(F.col("record_id"), F.col("cluster_id_p1")).alias("cluster_id"),
        )
    )

    # Pass 2 – propagate cluster_id through the edges once more
    pass2 = (
        pass1.alias("p")
        .join(edges.alias("e"), F.col("p.record_id") == F.col("e.src"), "left")
        .join(pass1.alias("n"), F.col("e.dst") == F.col("n.record_id"), "left")
        .groupBy("p.record_id")
        .agg(
            F.min(
                F.coalesce(F.col("n.cluster_id"), F.col("p.cluster_id"))
            ).alias("cluster_id_p2")
        )
        .select(
            F.col("record_id"),
            F.least(F.col("record_id"), F.col("cluster_id_p2")).alias("cluster_id"),
        )
    )

    return pass2


# ---------------------------------------------------------------------------
# 4.  SURVIVORSHIP  –  apply all rules per cluster
# ---------------------------------------------------------------------------
@dlt.table(
    name="mvw_mdm_survivorship",
    comment="Survived (golden) attribute per cluster after all rules",
    table_properties={"pipelines.reset.allowed": "true"},
)
def mvw_mdm_survivorship():
    staging  = dlt.read("mvw_mdm_combined_staging")
    clusters = dlt.read("mvw_mdm_clusters")

    # Enrich staging with cluster_id
    enriched = (
        staging.drop("cluster_id")
        .join(clusters, on="record_id", how="left")
    )

    # ── Rule 1 : FULL NAME (with prefix/suffix preserved) ──────────────────
    # FILTER nulls FIRST → rank by length DESC → then updated_at DESC
    # Use ORIGINAL customer_name to preserve "Dr.", "Jr.", etc. in golden record
    w_name = (
        Window.partitionBy("cluster_id")
              .orderBy(
                  F.length(F.col("customer_name")).desc_nulls_last(),
                  F.col("updated_at").desc_nulls_last()
              )
    )
    name_survived = (
        enriched
        .filter(F.col("customer_name").isNotNull())
        .withColumn("rn", F.row_number().over(w_name))
        .filter(F.col("rn") == 1)
        .select("cluster_id", F.col("customer_name").alias("survived_name"))
    )

    # ── Rule 2 : PREFERRED EMAIL ───────────────────────────────────────────
    # FILTER nulls FIRST → rank by domain rank ASC → then updated_at DESC
    w_email = (
        Window.partitionBy("cluster_id")
              .orderBy(
                  email_rank(F.col("email")).asc(),
                  F.col("updated_at").desc_nulls_last(),
              )
    )
    email_survived = (
        enriched
        .filter(F.col("email").isNotNull())
        .withColumn("rn", F.row_number().over(w_email))
        .filter(F.col("rn") == 1)
        .select("cluster_id", F.col("email").alias("survived_email"))
    )

    # ── Rule 3 : BEST FORMATTED PHONE (as-is from source) ──────────────────
    # FILTER nulls and invalid (< 7 digits) FIRST
    # Rank by: format quality DESC → digit count DESC → updated_at DESC
    # Use ORIGINAL phone value (don't apply formatting)
    w_phone = (
        Window.partitionBy("cluster_id")
              .orderBy(
                  phone_format_score(F.col("phone")).desc_nulls_last(),
                  digit_count(F.col("phone")).desc_nulls_last(),
                  F.col("updated_at").desc_nulls_last(),
              )
    )
    phone_survived = (
        enriched
        .filter(F.col("phone").isNotNull())
        .filter(digit_count(F.col("phone")) >= 7)
        .withColumn("rn", F.row_number().over(w_phone))
        .filter(F.col("rn") == 1)
        .select("cluster_id", F.col("phone").alias("survived_phone"))
    )

    # ── Rule 4 : RESOLVED ADDRESS ──────────────────────────────────────────
    # FILTER nulls FIRST → rank by length DESC → then updated_at DESC
    w_addr = (
        Window.partitionBy("cluster_id")
              .orderBy(
                  F.length(F.col("address")).desc_nulls_last(),
                  F.col("updated_at").desc_nulls_last(),
              )
    )
    addr_survived = (
        enriched
        .filter(F.col("address").isNotNull())
        .withColumn("expanded_addr", expand_address(F.col("address")))
        .withColumn("rn", F.row_number().over(w_addr))
        .filter(F.col("rn") == 1)
        .select("cluster_id", F.col("expanded_addr").alias("survived_address"))
    )

    # ── Rule 5 : RESOLVED POSTAL CODE ─────────────────────────────────────
    # FILTER nulls FIRST → rank by length DESC → then updated_at DESC
    w_postal = (
        Window.partitionBy("cluster_id")
              .orderBy(
                  F.length(F.col("postal_code")).desc_nulls_last(),
                  F.col("updated_at").desc_nulls_last()
              )
    )
    postal_survived = (
        enriched
        .filter(F.col("postal_code").isNotNull())
        .filter(F.trim(F.col("postal_code")) != "")
        .withColumn("rn", F.row_number().over(w_postal))
        .filter(F.col("rn") == 1)
        .select("cluster_id", F.col("postal_code").alias("survived_postal_code"))
    )

    # ── Rule 6 : CREDIT CARD ───────────────────────────────────────────────
    # FILTER nulls FIRST → rank by updated_at DESC (most recent)
    w_credit_card = (
        Window.partitionBy("cluster_id")
              .orderBy(F.col("updated_at").desc_nulls_last())
    )
    credit_card_survived = (
        enriched
        .filter(F.col("credit_card").isNotNull())
        .filter(F.trim(F.col("credit_card")) != "")
        .withColumn("rn", F.row_number().over(w_credit_card))
        .filter(F.col("rn") == 1)
        .select("cluster_id", F.col("credit_card").alias("survived_credit_card"))
    )

    # ── Assemble survivorship record ───────────────────────────────────────
    all_clusters = enriched.select("cluster_id").distinct()

    survived = (
        all_clusters
        .join(name_survived,        on="cluster_id", how="left")
        .join(email_survived,       on="cluster_id", how="left")
        .join(phone_survived,       on="cluster_id", how="left")
        .join(addr_survived,        on="cluster_id", how="left")
        .join(postal_survived,      on="cluster_id", how="left")
        .join(credit_card_survived, on="cluster_id", how="left")
    )

    return survived


# ---------------------------------------------------------------------------
# 5.  TARGET TABLE 1 – GOLDEN RECORD
#     electroniz_catalog.gold.mvw_electroniz_gold_golden_record_customers
# ---------------------------------------------------------------------------
@dlt.table(
    name="mvw_electroniz_gold_golden_record_customers",
    comment=(
        "MDM Golden Record: one unified row per matched-entity cluster. "
        "unified_id is the authoritative identifier."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "true",
    },
)
@dlt.expect_or_drop("valid_cluster_id", "cluster_id IS NOT NULL")
def mvw_electroniz_gold_golden_record_customers():
    survived = dlt.read("mvw_mdm_survivorship")

    # Rule 7 : assign a new unified_id  (deterministic UUID from cluster_id)
    return (
        survived
        .withColumn(
            "unified_id",
            F.concat(F.lit("GR-"), F.md5(F.col("cluster_id"))),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "unified_id",
            "cluster_id",
            F.col("survived_name").alias("resolved_full_name"),
            F.col("survived_email").alias("preferred_email"),
            F.col("survived_phone").alias("phone"),
            F.col("survived_address").alias("resolved_address"),
            F.col("survived_postal_code").alias("resolved_postal_code"),
            F.col("survived_credit_card").alias("credit_card"),
            "created_at",
        )
    )


# ---------------------------------------------------------------------------
# 6.  TARGET TABLE 2 – LINKING RECORD
#     electroniz_catalog.gold.mvw_electroniz_gold_linking_record_customers
# ---------------------------------------------------------------------------
@dlt.table(
    name="mvw_electroniz_gold_linking_record_customers",
    comment=(
        "MDM Linking Table: maps each source record_id "
        "to the cluster it belongs to and the resulting unified_id."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "true",
    },
)
@dlt.expect_or_drop("valid_record_id",  "record_id  IS NOT NULL")
@dlt.expect_or_drop("valid_unified_id", "unified_id IS NOT NULL")
def mvw_electroniz_gold_linking_record_customers():
    staging   = dlt.read("mvw_mdm_combined_staging")
    clusters  = dlt.read("mvw_mdm_clusters")
    golden    = dlt.read("mvw_electroniz_gold_golden_record_customers")

    # Join staging → cluster → golden to get unified_id per source record
    linked = (
        staging
        .drop("cluster_id")
        .join(clusters, on="record_id", how="left")
        .join(
            golden.select("cluster_id", "unified_id"),
            on="cluster_id",
            how="left",
        )
        .select(
            "record_id",
            "customer_id",
            "_source",
            "cluster_id",
            "unified_id",
            F.col("customer_name").alias("source_customer_name"),
            F.col("email").alias("source_email"),
            F.col("phone").alias("source_phone"),
            F.col("address").alias("source_address"),
            F.col("postal_code").alias("source_postal_code"),
            F.col("credit_card").alias("source_credit_card"),
            F.col("updated_at").alias("source_updated_at"),
            F.current_timestamp().alias("linked_at"),
        )
    )

    return linked
