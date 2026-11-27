const assert = require('node:assert/strict')
const { execFile } = require('node:child_process')
const { promisify } = require('node:util')
const { readFileSync } = require('node:fs')

const runCLI = promisify(execFile)
const config = JSON.parse(readFileSync(0, 'utf8').replace(/^\uFEFF/, ''))
const { chromium } = require(config.playwright)

const approvals = {
  start: 'credit-review',
  steps: [
    { key: 'credit-review', kind: 'approval', role: 'purchasing', after_seconds: 3600, next: 'purchase-review', on_failure: 'rejected', on_timeout: 'credit-overdue' },
    { key: 'purchase-review', kind: 'approval', role: 'purchasing', after_seconds: 7200, next: 'approved', on_failure: 'rejected', on_timeout: 'purchase-overdue' },
    { key: 'approved', kind: 'end', result: 'succeeded' },
    { key: 'rejected', kind: 'end', result: 'failed' },
    { key: 'credit-overdue', kind: 'end', result: 'failed' },
    { key: 'purchase-overdue', kind: 'end', result: 'failed' },
  ],
}

const raceDefinition = {
  start: 'release',
  steps: [
    { key: 'release', kind: 'approval', role: 'purchasing', after_seconds: 3600, next: 'cleared', on_failure: 'rejected', on_timeout: 'timed-out' },
    { key: 'cleared', kind: 'end', result: 'succeeded' },
    { key: 'rejected', kind: 'end', result: 'failed' },
    { key: 'timed-out', kind: 'end', result: 'failed' },
  ],
}

async function signIn(browser, credential) {
  const context = await browser.newContext({ baseURL: config.origin, viewport: { width: 1440, height: 900 } })
  const page = await context.newPage()
  await page.goto('/login')
  await page.getByLabel('登录凭据').fill(credential)
  const [response] = await Promise.all([
    page.waitForResponse(res => res.request().method() === 'POST' && res.url().endsWith('/login')),
    page.getByRole('button', { name: '登录', exact: true }).click(),
  ])
  assert.equal(response.status(), 303, 'process console sign-in')
  await page.waitForURL(config.origin + '/')
  return page
}

async function checkDesigner(page) {
  await page.goto('/process-designer')
  await page.getByLabel('流程键').fill('browser-purchasing')
  await page.getByLabel('导入 JSON 文件').setInputFiles({
    name: 'purchasing.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify(approvals)),
  })
  await page.locator('[data-process-message]').waitFor({ state: 'visible' })
  await page.getByRole('button', { name: '检查并预览' }).click()
  await page.locator('svg.dag').waitFor()
  const table = await page.getByRole('row').allTextContents()
  assert.ok(table.some(row => row.includes('credit-review') && row.includes('credit-overdue')))
  assert.ok(table.some(row => row.includes('purchase-review') && row.includes('purchase-overdue')))
  const unbound = {
    start: 'create-order', operations: ['create'],
    steps: [
      { key: 'create-order', kind: 'action', operation: 'create', compensation: 'missing', next: 'done', on_failure: 'failed' },
      { key: 'done', kind: 'end', result: 'succeeded' }, { key: 'failed', kind: 'end', result: 'failed' },
    ],
  }
  await page.getByLabel('流程定义 JSON').fill(JSON.stringify(unbound))
  const [bad] = await Promise.all([
    page.waitForResponse(res => res.request().method() === 'POST' && res.url().endsWith('/process-designer')),
    page.getByRole('button', { name: '发布版本' }).click(),
  ])
  assert.equal(bad.status(), 422)
  await page.getByText('unbound_compensation', { exact: false }).waitFor()
  await page.getByLabel('流程键').fill('browser-race')
  await page.getByLabel('流程定义 JSON').fill(JSON.stringify(raceDefinition))
  await page.getByRole('button', { name: '发布版本' }).click()
  await page.waitForURL(url => url.pathname === '/processes/browser-race' && url.searchParams.get('revision') === '1')
}

async function getDetail(page, id) {
  const response = await page.request.get(`/process-instances/${id}/report.json`)
  assert.equal(response.status(), 200)
  return response.json()
}

async function browserApprovalRace(operator, buyer, order) {
  await operator.goto('/processes/browser-race')
  await operator.getByLabel('业务键').fill(order)
  await operator.getByRole('button', { name: '启动实例' }).click()
  await operator.waitForURL('**/process-instances/*')
  const id = Number(operator.url().split('/').pop())
  assert.ok(id > 0)
  await operator.locator('[data-process-countdown]').filter({ hasText: '剩余' }).waitFor()
  const initial = await getDetail(operator, id)
  const task = initial.tasks[0]
  const wait = initial.waits[0]
  await buyer.goto('/process-tasks')
  const box = buyer.locator(`[data-task-id="${task.task_id}"]`)
  await box.getByRole('button', { name: '领取任务' }).click()
  const decision = box.locator('form').filter({ has: buyer.getByRole('button', { name: '同意', exact: true }) })
  const form = { action: 'approve', note: `acceptance-${order}` }
  for (const field of ['csrf', 'request', 'revision']) form[field] = await decision.locator(`[name="${field}"]`).inputValue()
  const approve = () => buyer.request.post(`/process-tasks/${task.task_id}`, { form, maxRedirects: 0 })
  const timer = () => runCLI(config.binary, ['scheduler', '--once', '--at', wait.deadline], { windowsHide: true })
  let response
  if (order === 'approval-first') {
    response = await approve()
    await timer()
  } else if (order === 'timer-first') {
    await timer()
    response = await approve()
  } else {
    [response] = await Promise.all([approve(), timer()])
  }
  const status = response.status()
  assert.ok([303, 409].includes(status), `approval status ${status}`)
  assert.equal(order === 'approval-first' ? status : order === 'timer-first' ? status : 303, order === 'timer-first' ? 409 : 303)
  await timer()
  await operator.reload()
  await operator.locator('[data-process-state]').filter({ hasText: status === 303 ? 'succeeded' : 'failed' }).waitFor()
  const final = await getDetail(operator, id)
  const persisted = JSON.parse((await runCLI(config.binary, ['process-show', 'demo', `${id}`], { windowsHide: true })).stdout)
  assert.equal(final.run.transition_rev, 2)
  assert.equal(final.run.step_key, status === 303 ? 'cleared' : 'timed-out')
  assert.equal(final.events.filter(event => event.kind === 'run_finished').length, 1)
  assert.equal(final.events.filter(event => event.kind === 'wait_resolved').length, 1)
  assert.deepEqual(final, persisted)
  const timerKey = `timer:${wait.wait_id}`
  assert.equal(final.events.filter(event => event.request_key === timerKey && event.kind === (status === 303 ? 'timer_late' : 'wait_resolved')).length, 1)
  assert.equal(final.events.filter(event => event.request_key === form.request && event.kind === (status === 303 ? 'task_approve' : 'task_input_rejected')).length, 1)
  return {
    order, id, task: task.task_id,
    approval_input: { action: form.action, note: form.note, revision: Number(form.revision), request: form.request },
    timer_input: { deadline: wait.deadline, wait_id: wait.wait_id, request: timerKey },
    approval_http: status, wait_winner: final.waits[0].winner_key, result: final.run.status, step: final.run.step_key,
    audit: final.events.filter(event => event.request_key === timerKey || event.request_key === form.request),
  }
}

async function main() {
  const browser = await chromium.launch({ executablePath: config.chrome || undefined, headless: true })
  try {
    const operator = await signIn(browser, config.operator)
    const buyer = await signIn(browser, config.buyer)
    const viewer = await signIn(browser, config.viewer)
    await checkDesigner(operator)
    const records = []
    for (const order of ['approval-first', 'timer-first', 'parallel']) records.push(await browserApprovalRace(operator, buyer, order))
    const forbidden = await viewer.request.get(`/process-instances/${records[0].id}/report.json`)
    assert.equal(forbidden.status(), 404)
    await viewer.goto('/processes')
    assert.equal(await viewer.getByRole('link', { name: 'browser-race', exact: true }).count(), 0)
    process.stdout.write(JSON.stringify({ browser: browser.version(), a1: 'two distinct timeout exits', a2_http: 422, scope_http: 404, records }) + '\n')
  } finally {
    await browser.close()
  }
}

main().catch(error => {
  process.stderr.write(error.stack + '\n')
  process.exitCode = 1
})
