"""Shared geometry utilities."""
import math


def is_point_in_polygon(lat: float, lng: float, polygon: list[tuple[float, float]]) -> bool:
    inside = False
    n = len(polygon)
    if n < 3:
        return False

    for i in range(n):
        j = (i - 1) % n
        xi, yi = polygon[i][0], polygon[i][1]
        xj, yj = polygon[j][0], polygon[j][1]

        if not (math.isfinite(xi) and math.isfinite(yi) and math.isfinite(xj) and math.isfinite(yj)):
            continue

        intersects = ((yi > lat) != (yj > lat)) and (
            lng < ((xj - xi) * (lat - yi)) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside

    return inside
