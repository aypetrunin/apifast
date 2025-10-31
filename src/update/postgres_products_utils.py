"""Модуль формирования полного названия услуги и типа услуги для компании Алиса.

Связано с особенностями формирования названий в Юклайнс.
"""

import asyncio
import re

import asyncpg

from ..common import logger  # type: ignore
from ..settings import settings  # type: ignore

POSTGRES_CONFIG = settings.postgres_config

# === Конфигурация ===
settings = {
    "keepOriginalCase": True,
    "maxNameLength": 180,
    "testMode": False,
}

service_value_map = {
    "прайс алисы викторовны": "Лазерная эпиляция.Прайс Алисы Викторовны",
    "токовые процедуры": "Аппаратные процедуры.Токовые",
    "фракционная мезотерапия лица": "Косметология.Мезотерапия.Фракционная",
    "уходовые процедуры для лица. косметология": "Уходовые процедуры для лица",
    "уходовые процедуры для тела": "Уходовые процедуры для тела",
    "уходовые процедуры для головы": "Уходовые процедуры для головы",
    "пилинги": "Косметология.Пилинги",
    "перманентный макияж": "Перманент.Макияж",
    "удаление": "Удаление.Пигмент/Татуаж",
    "художественная татуировка": "Тату.Мини",
    "комплексы по коррекции фигуры": "Коррекция фигуры.Комплексы",
    "миостимуляция + гальванизация": "Аппаратные процедуры.Миостимуляция+Гальванизация",
    "лазерное омоложение кожи": "Лазерные процедуры.Омоложение",
    "ручной массаж": "Массажи.Ручной",
    "руки": "Лазерная эпиляция.Руки",
    "лицо": "Лазерная эпиляция.Лицо",
    "тело": "Лазерная эпиляция.Тело",
    "ноги": "Лазерная эпиляция.Ноги",
    "мужская лэ": "Лазерная эпиляция.Мужская",
    "интимные зоны": "Лазерная эпиляция.Интим",
    "маски": "Косметология.Маски",
    "чистки лица": "Косметология.Чистки",
    "комплексы": "Коррекция фигуры.Комплексы",
    "lpg массаж": "Аппаратные процедуры.LPG",
    "консультация": "Сервис.Консультация",
}

massage_subtype_rules = [
    {"path": "Массажи.Спина", "includes": ["спин", "поясниц", "шейно", "воротник"]},
    {"path": "Массажи.Антицеллюлитный", "includes": ["антицеллюлит"]},
    {"path": "Массажи.Расслабляющий", "includes": ["релакс", "расслаб"]},
    {"path": "Массажи.Ноги", "includes": ["ног", "стоп"]},
]

permanent_keywords = [
    "перманент",
    "межреснич",
    "пудров",
    "стрелк",
    "бров",
    "татуаж",
    "веко",
    "губ",
]
hardware_keywords = ["lpg", "миостимул", "гальван", "токов", "кавитац", "rf", "вакуум"]
care_keywords = [
    "уход",
    "маска",
    "концентрат",
    "лифт",
    "увлажн",
    "осветлен",
    "регенерац",
    "фарфоровая куколка",
    "экспресс- уход",
    "экспресс-уход",
]
removal_keywords = ["удаление", "ремувер"]
tattoo_keywords = ["микро-тату", "мини-тату", "тату ", "татуиров"]
laser_zone_keywords = [
    "бикини",
    "подмыш",
    "ноги",
    "ногу",
    "голен",
    "рук",
    "руки",
    "предплеч",
    "бедр",
    "бедро",
    "линия живота",
    "живот",
    "плеч",
    "кист",
    "стоп",
    "пальц",
    "колен",
]
non_laser_complex_noise = [
    "массаж",
    "антицел",
    "lpg",
    "миостимул",
    "кавитац",
    "вакуум",
    "rf",
    "курс",
    "программ",
    "гальван",
]


# === Вспомогательные функции ===
def to_lower(v):  # type: ignore
    """Вспомогательная функция."""
    return str(v or "").strip().lower()


def normalize_spaces(s):  # type: ignore
    """Вспомогательная функция."""
    return re.sub(r"\s+", " ", s or "").strip()


def soft_cap(s, limit):  # type: ignore
    """Вспомогательная функция."""
    return s if not limit or len(s) <= limit else s[: limit - 1].strip() + "…"


def sanitize_name(s):  # type: ignore
    """Вспомогательная функция."""
    n = normalize_spaces(s)
    if not settings["keepOriginalCase"]:
        n = n.capitalize()
    return soft_cap(n, settings["maxNameLength"])


def massage_subtype(base_path, name_lower):  # type: ignore
    """Вспомогательная функция."""
    for rule in massage_subtype_rules:
        if any(k in name_lower for k in rule["includes"]):
            return rule["path"]
    return base_path


def extend_permanent(base_path, name_lower):  # type: ignore
    """Вспомогательная функция."""
    return f"{base_path}.Коррекция" if "коррекц" in name_lower else base_path


def count_matches(s, arr):  # type: ignore
    """Вспомогательная функция."""
    return sum(k in s for k in arr)


def is_laser_epilation_complex(name_lower, svc_lower, checkpoints=None):  # type: ignore
    """Возвращает True/False. Если передан checkpoints (list), добавляет пояснения."""

    def cp(msg):  # type: ignore
        """Вспомогательная функция."""
        if checkpoints is not None:
            checkpoints.append(msg)

    cp("is_laser_epilation_complex: start")
    zone_count = count_matches(name_lower, laser_zone_keywords)
    cp(f"is_laser_epilation_complex: zone_count={zone_count}")
    if zone_count < 2:
        cp("is_laser_epilation_complex: zone_count < 2 -> False")
        return False

    noise_matches = [k for k in non_laser_complex_noise if k in name_lower]
    cp(f"is_laser_epilation_complex: non-laser noise matches: {noise_matches}")
    if noise_matches:
        cp("is_laser_epilation_complex: noise found -> False")
        return False

    sizePattern = re.search(r"\b(xs\+?|s|m\+?|m\s*\+|l\+?|l)\b", name_lower, flags=re.I)
    comboPattern = re.search(r"\(.+\+.+\)", name_lower)
    cp(
        f"is_laser_epilation_complex: sizePattern={bool(sizePattern)}, comboPattern={bool(comboPattern)}"
    )

    if not (sizePattern or comboPattern):
        cp("is_laser_epilation_complex: no size/combo pattern -> False")
        return False

    cp("is_laser_epilation_complex -> True")
    return True


def classify(product_name, service_value, description, debug: bool = False):  # type: ignore
    """Возвращает категорию. Если debug=True возвращает dict {'category':..., 'checkpoints':[...]}.

    Контрольные точки добавлены на всех ключевых шагах.
    """
    checkpoints = []

    def cp(msg):  # type: ignore
        checkpoints.append(msg)

    svc_lower = to_lower(service_value)
    name_lower = to_lower(product_name)
    desc_lower = to_lower(description)
    all_lower = f"{name_lower} {svc_lower} {desc_lower}"

    cp(
        f"INPUT: product_name='{product_name}' | service_value='{service_value}' | description='{description}'"
    )
    cp(
        f"LOWER: name_lower='{name_lower}' | svc_lower='{svc_lower}' | desc_lower='{desc_lower}'"
    )

    category = None

    # 1) Service value map
    if svc_lower in service_value_map:
        mapped = service_value_map[svc_lower]
        cp(f"SERVICE_MAP HIT: svc_lower='{svc_lower}' -> mapped='{mapped}'")
        category = mapped

        if category.startswith("Перманент.Макияж"):
            old = category
            category = extend_permanent(category, name_lower)
            cp(f"extend_permanent: '{old}' -> '{category}'")

        if category.startswith("Массажи."):
            old = category
            category = massage_subtype(category, name_lower)
            cp(f"massage_subtype applied: '{old}' -> '{category}'")

        if (
            category == "Коррекция фигуры.Комплекс"
            or category == "Коррекция фигуры.Комплексы"
        ):
            cp(
                "service_map category is Коррекция фигуры.Комплексы -> checking laser complex override"
            )
            laser = is_laser_epilation_complex(name_lower, svc_lower, checkpoints)
            cp(f"is_laser_epilation_complex returned {laser}")
            if laser:
                category = "Лазерная эпиляция.Комплексы"
                cp("OVERRIDE: set category -> 'Лазерная эпиляция.Комплексы'")

    # 2) Permanent keywords
    if not category:
        matched = [k for k in permanent_keywords if k in all_lower]
        cp(f"PERMANENT check: matches={matched}")
        if matched:
            category = extend_permanent("Перманент.Макияж", name_lower)
            cp(f"Set category (PERMANENT): '{category}'")

    # 3) Massage by name
    if not category and "массаж" in name_lower:
        cp("Massage keyword found in name -> applying massage_subtype")
        category = massage_subtype("Массажи.Ручной", name_lower)
        cp(f"Set category (MASSAGE): '{category}'")

    # 4) Laser rejuvenation
    if not category and "лазер" in all_lower and "омолож" in all_lower:
        category = "Лазерные процедуры.Омоложение"
        cp("Set category (LASER_REJUV): 'Лазерные процедуры.Омоложение'")

    # 5) Hardware keywords branch
    if not category and any(k in all_lower for k in hardware_keywords):
        hw_matched = [k for k in hardware_keywords if k in all_lower]
        cp(f"HARDWARE check: hardware keywords matched: {hw_matched}")

        if "миостимул" in all_lower and "гальван" in all_lower:
            category = "Аппаратные процедуры.Миостимуляция+Гальванизация"
            cp("Set category: Миостимуляция+Гальванизация")
        elif any(x in all_lower for x in ("токов", "гальван", "микроток")):
            category = "Аппаратные процедуры.Токовые"
            cp("Set category: Токовые (токов|гальван|микроток)")
        elif "кавитац" in all_lower:
            category = "Аппаратные процедуры.Кавитация"
            cp("Set category: Кавитация")
        elif "lpg" in all_lower:
            category = "Аппаратные процедуры.LPG"
            cp("Set category: LPG")
        elif "rf" in all_lower:
            category = "Аппаратные процедуры.RF"
            cp("Set category: RF")
        else:
            category = "Аппаратные процедуры.Общее"
            cp("Set category: Аппаратные процедуры.Общее")

    # 6) Peelings
    if not category and "пилинг" in all_lower:
        category = "Косметология.Пилинги"
        cp("Set category: Косметология.Пилинги")

    # 7) Mesotherapy
    if not category and "мезотерап" in all_lower:
        category = "Косметология.Мезотерапия.Общая"
        cp("Set category: Косметология.Мезотерапия.Общая")

    # 8) Care keywords
    if not category and any(k in all_lower for k in care_keywords):
        care_matches = [k for k in care_keywords if k in all_lower]
        cp(f"CARE check: matched {care_matches}")
        if re.search(r"лицо|век|губ", all_lower):
            category = "Косметология.Уход.Лицо"
            cp("Set category: Косметология.Уход.Лицо")
        elif re.search(r"тело|рук|ног|спин|поясниц", all_lower):
            category = "Косметология.Уход.Тело"
            cp("Set category: Косметология.Уход.Тело")
        else:
            category = "Косметология.Уход.Общее"
            cp("Set category: Косметология.Уход.Общее")

    # 9) Course/complex detection (body complexes)
    if not category:
        is_course = bool(re.search(r"(курс|комплекс|программ)", all_lower))
        has_tech = bool(
            re.search(r"(lpg|миостимул|кавитац|вакуум|rf|целлюлит)", all_lower)
        )
        cp(f"COURSE check: is_course={is_course}, has_tech={has_tech}")
        if is_course and has_tech:
            category = "Коррекция фигуры.Комплексы"
            cp("Set category: Коррекция фигуры.Комплексы (course + tech)")

    # 10) Late override for laser complexes
    if category == "Коррекция фигуры.Комплексы":
        cp("Late override check for laser epilation complexes")
        laser = is_laser_epilation_complex(name_lower, svc_lower, checkpoints)
        cp(f"is_laser_epilation_complex returned {laser} (late override)")
        if laser:
            category = "Лазерная эпиляция.Комплексы"
            cp("Late override applied -> Лазерная эпиляция.Комплексы")

    # 11) Removal
    if not category and any(k in all_lower for k in removal_keywords):
        rem = [k for k in removal_keywords if k in all_lower]
        cp(f"REMOVAL check: matches={rem}")
        category = "Удаление.Пигмент/Татуаж"
        cp("Set category: Удаление.Пигмент/Татуаж")

    # 12) Tattoo
    if not category and any(k in all_lower for k in tattoo_keywords):
        tat = [k for k in tattoo_keywords if k in all_lower]
        cp(f"TATTOO check: matches={tat}")
        category = "Тату.Мини"
        cp("Set category: Тату.Мини")

    # 13) Consultation
    if not category and "консультац" in all_lower:
        category = "Сервис.Консультация"
        cp("Set category: Сервис.Консультация")

    # 14) Fallback
    if not category:
        category = "Прочее"
        cp("Fallback -> Прочее")

    # Нормализация и финал
    normalized = ".".join([s.strip() for s in category.split(".") if s.strip()])
    cp(f"FINAL category (normalized): '{normalized}'")

    if debug:
        return {"category": normalized, "checkpoints": checkpoints}
    return normalized


# === Тестовая функция ===
async def test_classification(channel_id: int = 2, limit: int = 10):  # type: ignore
    """Тестирует классификацию без обновления таблицы.

    Выводит результаты в консоль.
    """
    conn = await asyncpg.connect(**POSTGRES_CONFIG)
    try:
        rows = await conn.fetch(
            "SELECT product_id, product_name, service_value, description "
            "FROM products WHERE channel_id=$1 LIMIT $2",
            channel_id,
            limit,
        )

        logger.info(
            f"🔍 Тест классификации для channel_id={channel_id} (первые {limit} записей):\n"
        )

        for r in rows:
            category = classify(
                r["product_name"],
                r["service_value"],
                r["description"] or "",
                debug=False,
            )
            display_name = sanitize_name(r["product_name"])
            full_name = f"{category} - {display_name}"

            logger.info(f"🧩 ID: {r['product_id']}")
            logger.info(f"  → category: {category}")
            logger.info(f"  → product_full_name: {full_name}\n")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(test_classification())


# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update.postgres_products_utils
