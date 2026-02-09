from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class Bookmark:
    id: str
    title: str
    url: str
    add_date: Optional[int] = None
    last_modified: Optional[int] = None
    folder_path: List[str] = field(default_factory=list)

    domain: str = ""
    lang: str = "EN"
    fetched_ok: Optional[bool] = None
    http_status: Optional[int] = None
    final_url: Optional[str] = None
    page_title: Optional[str] = None
    page_description: Optional[str] = None
    content_snippet: Optional[str] = None
    page_html: Optional[str] = None

    summary: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    assigned_path: List[str] = field(default_factory=list)
    assigned_title: Optional[str] = None

    meta: Dict[str, str] = field(default_factory=dict)


@dataclass
class FolderNode:
    name: str
    sort_key: str = ""
    children: Dict[str, "FolderNode"] = field(default_factory=dict)
    bookmarks: List[Bookmark] = field(default_factory=list)
    path_tokens: List[str] = field(default_factory=list)

    def get_or_create(self, name: str) -> "FolderNode":
        if name not in self.children:
            self.children[name] = FolderNode(name=name, sort_key=name)
        return self.children[name]
