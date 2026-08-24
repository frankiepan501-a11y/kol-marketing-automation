const response = $input.first().json || {};
const apiError = response.error && response.error.message
  ? String(response.error.message)
  : '';
const content = response.choices && response.choices[0] && response.choices[0].message
  ? response.choices[0].message.content
  : null;

if (typeof content !== 'string' || !content.trim()) {
  throw new Error(
    `[SEO_RESPONSE_GATE] DeepSeek returned no article content` +
    (apiError ? `; api_error=${apiError}` : '')
  );
}

const article1Markers = (content.match(/===ARTICLE_1===/gi) || []).length;
const article2Markers = (content.match(/===ARTICLE_2===/gi) || []).length;
const firstPosition = content.search(/===ARTICLE_1===/i);
const secondPosition = content.search(/===ARTICLE_2===/i);
const firstBody = firstPosition >= 0 && secondPosition > firstPosition
  ? content.slice(firstPosition + '===ARTICLE_1==='.length, secondPosition).trim()
  : '';
const secondBody = secondPosition >= 0
  ? content.slice(secondPosition + '===ARTICLE_2==='.length).trim()
  : '';
if (
  article1Markers !== 1 || article2Markers !== 1 ||
  firstPosition < 0 || secondPosition <= firstPosition ||
  !firstBody || !secondBody
) {
  throw new Error(
    `[SEO_RESPONSE_GATE] invalid two-article contract; ` +
    `article1_markers=${article1Markers}, article2_markers=${article2Markers}, ` +
    `article1_body=${firstBody.length}, article2_body=${secondBody.length}`
  );
}

return [{ json: { rawText: content.trim() } }];
