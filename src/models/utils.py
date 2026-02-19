from src.models.label import Label, LabelType, Language


def insert_short_label_to_labels_from_ref_labels(labels: list[Label], ref_lables: list[Label]) -> None:
    """Заполнить SHORT label из ref_lables в labels, если его нет."""
    ru_short = None
    en_short = None
    other_short = None
    for label in labels:
        if label.type == LabelType.SHORT:
            return None
    for ref_label in ref_lables:
        if ref_label.type == LabelType.SHORT and ref_label.type == Language.RU:
            ru_short = ref_label
        elif ref_label.type == LabelType.SHORT and ref_label.type == Language.EN:
            en_short = ref_label
        elif ref_label.type == LabelType.SHORT:
            other_short = ref_label
    appended_short = ru_short or en_short or other_short
    if appended_short:
        labels.append(appended_short)
    return None
