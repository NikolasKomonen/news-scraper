from yarl import URL


def resolve_hyperlink(origin_url: URL, hyperlink: str) -> URL:
    hyperlink_url = URL(hyperlink)
    if hyperlink_url.scheme:
        # Hyperlink is a full URL
        return hyperlink_url
    # Hyperlink is a path relative to the origin url
    return URL.build(scheme=origin_url.scheme, host=origin_url.host, path=hyperlink)
