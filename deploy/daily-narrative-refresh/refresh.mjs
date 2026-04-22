const baseUrl =
  process.env.FOCUS_API_REFRESH_BASE_URL ||
  process.env.API_BASE_URL ||
  'https://backend-production-02fa.up.railway.app';
const setupToken = process.env.SETUP_TOKEN;

if (!setupToken) {
  console.error('[focus-refresh] SETUP_TOKEN is not set');
  process.exit(1);
}

const endpoint = `${baseUrl.replace(/\/$/, '')}/ideas/focus/daily-narrative/refresh`;

console.log(`[focus-refresh] narrative refresh started (${endpoint})`);

try {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'x-setup-token': setupToken,
      'content-type': 'application/json',
    },
  });

  const rawBody = await response.text();

  if (!response.ok) {
    console.error(
      `[focus-refresh] narrative refresh failed (${response.status} ${response.statusText})`,
    );
    if (rawBody) {
      console.error(rawBody);
    }
    process.exit(1);
  }

  let parsedBody = null;
  if (rawBody) {
    try {
      parsedBody = JSON.parse(rawBody);
    } catch {
      parsedBody = rawBody;
    }
  }

  console.log(
    `[focus-refresh] narrative refreshed (${typeof parsedBody === 'string' ? parsedBody : JSON.stringify(parsedBody)})`,
  );
} catch (error) {
  console.error('[focus-refresh] narrative refresh errored');
  console.error(error);
  process.exit(1);
}
