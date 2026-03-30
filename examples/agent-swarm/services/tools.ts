/** Tool: Wikipedia article summary — returns extract, thumbnail, and URL. */
export async function wikipediaSummary(title: string): Promise<{ extract: string; thumbnail?: string; url: string }> {
  try {
    const resp = await fetch(`https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`);
    const data = await resp.json() as Record<string, unknown>;
    return {
      extract: (data.extract as string) || "",
      thumbnail: (data.thumbnail as Record<string, string> | undefined)?.source,
      url: ((data.content_urls as Record<string, Record<string, string>> | undefined)?.desktop?.page) || "",
    };
  } catch {
    return { extract: "", url: "" };
  }
}

/** Tool: Wikipedia search — returns up to 5 page title/excerpt pairs. */
export async function wikipediaSearch(query: string): Promise<Array<{ title: string; excerpt: string }>> {
  try {
    const resp = await fetch(`https://en.wikipedia.org/w/rest.php/v1/search/page?q=${encodeURIComponent(query)}&limit=5`);
    const data = await resp.json() as Record<string, unknown>;
    return ((data.pages as Array<Record<string, unknown>>) || []).map((p) => ({
      title: (p.title as string) || "",
      excerpt: (p.excerpt as string) || "",
    }));
  } catch {
    return [];
  }
}

/** Tool: Jina AI web search — returns raw text results. */
export async function jinaSearch(query: string): Promise<string> {
  try {
    const resp = await fetch(`https://s.jina.ai/${encodeURIComponent(query)}`, {
      headers: { Accept: "application/json" },
    });
    return await resp.text();
  } catch {
    return "";
  }
}

/** Tool: Jina AI reader — fetches and returns readable text content of a URL. */
export async function jinaRead(url: string): Promise<string> {
  try {
    const resp = await fetch(`https://r.jina.ai/${url}`);
    return await resp.text();
  } catch {
    return "";
  }
}

/** Tool: arXiv academic paper search — returns up to 5 papers with title, summary, and authors. */
export async function arxivSearch(query: string): Promise<Array<{ title: string; summary: string; authors: string }>> {
  try {
    const resp = await fetch(`https://export.arxiv.org/api/query?search_query=all:${encodeURIComponent(query)}&max_results=5`);
    const xml = await resp.text();
    const entries = xml.split("<entry>").slice(1);
    return entries.map((e) => ({
      title: (e.match(/<title>([\s\S]*?)<\/title>/)?.[1] || "").trim(),
      summary: (e.match(/<summary>([\s\S]*?)<\/summary>/)?.[1] || "").trim().slice(0, 300),
      authors: (e.match(/<name>([\s\S]*?)<\/name>/g) || []).map((n) => n.replace(/<\/?name>/g, "")).join(", "),
    }));
  } catch {
    return [];
  }
}

/** Tool: Hacker News search — returns up to 5 stories with title, URL, and points. */
export async function hackerNewsSearch(query: string): Promise<Array<{ title: string; url: string; points: number }>> {
  try {
    const resp = await fetch(`https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(query)}&hitsPerPage=5`);
    const data = await resp.json() as Record<string, unknown>;
    return ((data.hits as Array<Record<string, unknown>>) || []).map((h) => ({
      title: (h.title as string) || "",
      url: (h.url as string) || "",
      points: (h.points as number) || 0,
    }));
  } catch {
    return [];
  }
}

/** Tool: Google News RSS search — returns up to 5 news articles with title, source, and link. */
export async function googleNewsSearch(query: string): Promise<Array<{ title: string; source: string; link: string }>> {
  try {
    const resp = await fetch(`https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`);
    const xml = await resp.text();
    const items = xml.split("<item>").slice(1, 6);
    return items.map((item) => ({
      title: (item.match(/<title>([\s\S]*?)<\/title>/)?.[1] || "").replace(/<!\[CDATA\[|\]\]>/g, "").trim(),
      source: (item.match(/<source[^>]*>([\s\S]*?)<\/source>/)?.[1] || "").trim(),
      link: (item.match(/<link>([\s\S]*?)<\/link>/)?.[1] || "").trim(),
    }));
  } catch {
    return [];
  }
}

/** Tool: Reddit search — returns up to 5 posts with title, subreddit, upvotes, and URL. */
export async function redditSearch(query: string): Promise<Array<{ title: string; subreddit: string; upvotes: number; url: string }>> {
  try {
    const resp = await fetch(`https://www.reddit.com/search.json?q=${encodeURIComponent(query)}&limit=5&sort=relevance`, {
      headers: { "User-Agent": "agent-swarm-demo/1.0" },
    });
    const data = await resp.json() as Record<string, unknown>;
    return ((data.data as Record<string, unknown>)?.children as Array<Record<string, unknown>> || []).map((c) => {
      const d = c.data as Record<string, unknown>;
      return {
        title: (d?.title as string) || "",
        subreddit: (d?.subreddit as string) || "",
        upvotes: (d?.ups as number) || 0,
        url: `https://reddit.com${(d?.permalink as string) || ""}`,
      };
    });
  } catch {
    return [];
  }
}
