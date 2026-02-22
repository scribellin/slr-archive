const state = {
  stories: [],
  filtered: []
};

const els = {
  searchInput: document.getElementById("searchInput"),
  yearFilter: document.getElementById("yearFilter"),
  topicFilter: document.getElementById("topicFilter"),
  sortSelect: document.getElementById("sortSelect"),
  resultCount: document.getElementById("resultCount"),
  results: document.getElementById("results"),
  template: document.getElementById("storyCardTemplate")
};

initialize();

async function initialize() {
  try {
    const response = await fetch("./stories.json");
    if (!response.ok) {
      throw new Error("Failed to load stories");
    }

    const stories = await response.json();
    state.stories = normalizeStories(stories);

    populateFilters(state.stories);
    bindEvents();

    const params = new URLSearchParams(window.location.search);
    const q = params.get("q");
    if (q) {
      els.searchInput.value = q;
    }

    filterAndRender();
  } catch (error) {
    els.resultCount.textContent = "Could not load stories.json.";
    els.results.innerHTML = `<p class="empty-state">${error.message}</p>`;
  }
}

function normalizeStories(stories) {
  return stories
    .map((story) => ({
      ...story,
      issueDate: new Date(story.issueDate),
      headline: String(story.headline || "").trim(),
      outlet: String(story.outlet || "").trim(),
      writer: String(story.writer || "").trim(),
      topic: String(story.topic || "General").trim(),
      summary: sanitizeSummary(String(story.summary || "").trim()),
      issueTitle: String(story.issueTitle || "").trim(),
      url: String(story.url || "").trim(),
      issueUrl: String(story.issueUrl || "").trim(),
      leadImage: normalizeAssetUrl(String(story.leadImage || "").trim())
    }))
    .filter(
      (story) =>
        !Number.isNaN(story.issueDate.getTime()) && story.headline.length > 0
    );
}

function populateFilters(stories) {
  const years = [...new Set(stories.map((story) => story.issueDate.getFullYear()))].sort(
    (a, b) => b - a
  );
  const topics = [...new Set(stories.map((story) => story.topic).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b)
  );

  years.forEach((year) => {
    const option = document.createElement("option");
    option.value = String(year);
    option.textContent = String(year);
    els.yearFilter.append(option);
  });

  topics.forEach((topic) => {
    const option = document.createElement("option");
    option.value = topic;
    option.textContent = topic;
    els.topicFilter.append(option);
  });
}

function bindEvents() {
  [els.searchInput, els.yearFilter, els.topicFilter, els.sortSelect].forEach((el) => {
    el.addEventListener("input", filterAndRender);
    el.addEventListener("change", filterAndRender);
  });
}

function filterAndRender() {
  const searchValue = els.searchInput.value.trim().toLowerCase();
  const yearValue = els.yearFilter.value;
  const topicValue = els.topicFilter.value;
  const sortValue = els.sortSelect.value;

  const filtered = state.stories.filter((story) => {
    const matchesSearch =
      !searchValue ||
      [
        story.headline,
        story.outlet,
        story.writer,
        story.topic,
        story.summary,
        story.issueTitle
      ]
        .join(" ")
        .toLowerCase()
        .includes(searchValue);

    const matchesYear = !yearValue || story.issueDate.getFullYear() === Number(yearValue);
    const matchesTopic = !topicValue || story.topic === topicValue;

    return matchesSearch && matchesYear && matchesTopic;
  });

  state.filtered = sortStories(filtered, sortValue);
  renderResults(state.filtered);
}

function sortStories(stories, sortValue) {
  const copied = [...stories];

  if (sortValue === "oldest") {
    copied.sort((a, b) => a.issueDate - b.issueDate);
    return copied;
  }

  if (sortValue === "title") {
    copied.sort((a, b) => a.headline.localeCompare(b.headline));
    return copied;
  }

  copied.sort((a, b) => b.issueDate - a.issueDate);
  return copied;
}

function renderResults(stories) {
  const count = stories.length;
  els.resultCount.textContent = `${count} stor${count === 1 ? "y" : "ies"} found`;

  if (count === 0) {
    els.results.innerHTML = `<p class="empty-state">No stories match this search yet.</p>`;
    return;
  }

  const fragment = document.createDocumentFragment();
  stories.forEach((story, index) => {
    const node = els.template.content.cloneNode(true);
    const date = node.querySelector(".date");
    const leadImage = node.querySelector(".lead-image");
    const title = node.querySelector(".title");
    const publication = node.querySelector(".publication");
    const summary = node.querySelector(".summary");
    const tagRow = node.querySelector(".tag-row");
    const link = node.querySelector(".link");
    const card = node.querySelector(".card");

    date.textContent = formatDate(story.issueDate);
    title.textContent = story.headline;

    if (story.leadImage) {
      leadImage.src = story.leadImage;
      leadImage.alt = `Lead image for ${story.headline}`;
      leadImage.hidden = false;
    } else {
      leadImage.hidden = true;
    }

    const bylineParts = [story.outlet, story.writer].filter(Boolean);
    publication.textContent = bylineParts.join(" · ") || "Unknown outlet";

    summary.textContent = story.summary || "Description unavailable.";
    link.href = story.url || story.issueUrl || "#";

    card.style.animationDelay = `${Math.min(index * 35, 280)}ms`;

    if (story.topic) {
      const tag = document.createElement("span");
      tag.className = "tag";
      tag.textContent = story.topic;
      tagRow.append(tag);
    }

    fragment.append(node);
  });

  els.results.replaceChildren(fragment);
}

function sanitizeSummary(summary) {
  const cleaned = summary.replace(/\s+/g, " ").trim();
  if (!cleaned) {
    return "";
  }
  if (cleaned.toLowerCase().startsWith("recommended in the sunday long read")) {
    return "";
  }
  return cleaned;
}

function normalizeAssetUrl(url) {
  if (!url) {
    return "";
  }
  if (url.startsWith("http://")) {
    return `https://${url.slice(7)}`;
  }
  return url;
}

function formatDate(date) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(date);
}
