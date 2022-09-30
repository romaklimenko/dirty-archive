let previousMediaHistory = [];
let nextMediaHistory = [];
let media = null;

document.addEventListener('DOMContentLoaded', async function() {
  await nextMedia();
  document.addEventListener('keydown', keyDown);
});

function humanReadableSize(bytes) {
  const threshold = 1024;

  if (Math.abs(bytes) < threshold) {
    return bytes + ' B';
  }

  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  let u = -1;
  const r = 10;

  do {
    bytes /= threshold;
    ++u;
  } while (Math.round(Math.abs(bytes) * r) / r >= threshold && u < units.length - 1);

  return bytes.toFixed(1) + ' ' + units[u];
}

async function keyDown(keyboardEvent) {
  if (keyboardEvent.code === 'ArrowUp' || keyboardEvent.code === 'ArrowDown') {
    await reviewMedia(keyboardEvent.code === 'ArrowUp');
  }

  if (keyboardEvent.code === 'KeyW' || keyboardEvent.code === 'KeyS') {
    await reviewMedia(keyboardEvent.code === 'KeyW');
  }

  switch (keyboardEvent.code) {
    case 'ArrowLeft':
    case 'KeyA':
      await previousMedia();
      break;
    case 'ArrowRight':
    case 'KeyD':
      await nextMedia();
      break;
  }
}

async function getMedia() {
  const response = await fetch('/api/media');
  media = await response.json();
}

function render() {
  const randomMediaDiv = document.getElementById('random-media');
  const randomMediaUsageDiv = document.getElementById('random-media-usage');

  randomMediaDiv.innerHTML = '';
  randomMediaUsageDiv.innerHTML = '';

  let info =
  `<dl>
    <dt>Файл:</dt>
    <dd><a href="${media._id}" target="_blank">${media._id}</a><br>от ${ new Date(Math.max(...media.ts) * 1000).toISOString().substring(0, 19) }</dd>
    <dt>Размер:</dt>
    <dd>${humanReadableSize(media.length)}</dd>
    <dt>Используется:</dt>
    <dd>
      <ul>`;
  media.usage.forEach(url => {
    info += `<li><a href="${url}" target="_blank">${url}</a>`;
  });
  info += '</li></dd></dl>';
  randomMediaUsageDiv.innerHTML = info;

  if (media.content_type.startsWith('image')) {
    randomMediaDiv.innerHTML = `<img id="random-media-source" class="img-fluid" style="max-height: 80vh;" src="${media._id}" />`
  } else if (media.content_type.startsWith('video')) {
    randomMediaDiv.innerHTML =
    ` <video loop autoplay muted width="100%">
        <source id="random-media-source" src="${media._id}" type="${media.content_type}" />
      </video>`;
  }
}

async function nextMedia() {
  if (media) {
    previousMediaHistory.push({ ...media });
    previousMediaHistory = previousMediaHistory.slice(-10);
  }

  if (nextMediaHistory.length === 0) {
    await getMedia();
  } else {
    media = nextMediaHistory.pop();
  }

  console.log(media);
  render();
}

async function previousMedia() {
  if (previousMediaHistory.length === 0) {
    return;
  }
  nextMediaHistory.push({ ...media });
  nextMediaHistory.slice(-10);
  media = previousMediaHistory.pop();
  render();
}

async function reviewMedia(verdict) {
  const formData = new FormData();
  formData.append('hash', media.hash);
  formData.append('verdict', verdict);

  await fetch('/api/media/review', {
    method: 'POST',
    body: formData
  });

  await nextMedia();
}
