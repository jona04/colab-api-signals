def sanitize_for_bson(obj):
    if isinstance(obj, dict):
        return {k: sanitize_for_bson(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_bson(x) for x in obj]
    if isinstance(obj, int):
        # bounds de int64
        MAX_I64 = 2**63 - 1
        MIN_I64 = -2**63
        if obj > MAX_I64 or obj < MIN_I64:
            return float(obj)
    return obj