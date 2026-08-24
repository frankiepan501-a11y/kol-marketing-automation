const inp = $('Parse Both Articles').first().json || {};
const art = inp.__ARTICLE_KEY__ || {};
const html = String(art.html || '');
const issues = [];
let quality = 100;

function requireField(value, label, penalty) {
  if (!String(value || '').trim()) {
    issues.push(`${label} missing`);
    quality -= penalty;
  }
}

requireField(art.title, 'title', 30);
requireField(art.slug, 'slug', 20);
requireField(art.meta, 'meta', 15);
requireField(art.excerpt, 'excerpt', 10);
requireField(html, 'html', 50);

if (html.length < 3000) {
  issues.push(`html too short (${html.length} chars)`);
  quality -= 25;
}
if (!/<h1\b/i.test(html)) {
  issues.push('H1 missing');
  quality -= 15;
}
const h2Count = (html.match(/<h2\b/gi) || []).length;
if (h2Count < 5) {
  issues.push(`too few H2 sections (${h2Count})`);
  quality -= 15;
}
const sourcesMatch = html.match(/<h2\b[^>]*id=["']sources["'][^>]*>[\s\S]*?<\/h2>([\s\S]*?)(?=<h2\b|$)/i);
const sourcesHtml = sourcesMatch ? sourcesMatch[1] : '';
const sourceUrls = Array.from(sourcesHtml.matchAll(/<a\b[^>]*href=["'](https?:\/\/[^"']+)["']/gi), (match) => match[1]);
const ownDomains = ['powkong.com', 'funlabswitch.com', 'fireflyfunlab.com'];
const sourceDomains = new Set(sourceUrls.map((url) => {
  const match = String(url).match(/^https?:\/\/([^\/?#]+)/i);
  return match ? match[1].toLowerCase().replace(/^www\./, '') : '';
}).filter((host) => host && !ownDomains.some((own) => host === own || host.endsWith(`.${own}`))));
const sourceGateFailed = !sourcesMatch || sourceDomains.size < 3;
if (sourceGateFailed) {
  issues.push(`too few unique external source domains (${sourceDomains.size})`);
  quality -= 30;
}
const concreteFacts = (html.match(/\b(?:19|20)\d{2}\b|\$\s?\d|\b\d+(?:\.\d+)?%|\b\d+(?:\.\d+)?\s?(?:million|billion|GB|MB|Hz|ms)\b/gi) || []).length;
if (concreteFacts < 3) {
  issues.push(`too few concrete facts (${concreteFacts})`);
  quality -= 15;
}
if (String(art.meta || '').length > 170) {
  issues.push(`meta too long (${String(art.meta).length})`);
  quality -= 5;
}
if (/\b(?:delve|in today's fast-paced|game-changer|revolutionary landscape)\b/i.test(html)) {
  issues.push('generic AI-style phrase detected');
  quality -= 10;
}

quality = Math.max(0, Math.min(100, quality));
const criticalMissing = !art.title || !art.slug || !art.meta || !art.excerpt || !html;
const verdict = criticalMissing || sourceGateFailed || quality < 70 ? 'reject' : 'pass';
const aiTraceScore = /\b(?:delve|in today's fast-paced|game-changer|revolutionary landscape)\b/i.test(html) ? 70 : 10;

return [{
  json: {
    site: '__SITE__',
    title: art.title || '',
    ai_trace_score: aiTraceScore,
    quality_score: quality,
    verdict,
    qa_issues: issues,
  },
}];
