const editor = document.querySelector('[data-editor]')
if (editor) {
  const source = editor.querySelector('[data-source]')
  const message = editor.querySelector('[data-editor-message]')
  const graphEditor = editor.querySelector('[data-graph-editor]')
  const workflowKey = editor.querySelector('[data-workflow-key]')
  const nodeList = editor.querySelector('[data-node-list]')
  const nodeKey = editor.querySelector('[data-node-key]')
  const task = editor.querySelector('[data-node-task]')
  const queue = editor.querySelector('[data-node-queue]')
  const timeout = editor.querySelector('[data-node-timeout]')
  const depends = editor.querySelector('[data-depends]')
  const maxBytes = 1024 * 1024

  function readDefinition() {
    if (!source.value.trim()) return { key: '', nodes: [] }
    const definition = JSON.parse(source.value)
    if (typeof definition?.key !== 'string' || !Array.isArray(definition?.nodes) ||
        definition.nodes.some(node => typeof node?.key !== 'string' || (node.depends_on && !Array.isArray(node.depends_on)))) {
      throw new Error('定义须包含工作流键和节点数组。')
    }
    return definition
  }

  function fillNode(definition, selected) {
    const node = definition.nodes[selected]
    workflowKey.value = definition.key
    nodeKey.value = node?.key ?? ''
    task.value = node?.task ?? ''
    queue.value = node?.queue ?? 'default'
    timeout.value = node?.timeout_seconds ?? 60
    const edges = new Map((node?.depends_on ?? []).map(edge => [edge.node, edge]))
    const options = []
    for (const [index, dependency] of definition.nodes.entries()) {
      if (index === selected) continue
      const edge = edges.get(dependency.key)
      const caption = edge?.branch ?? edge?.when
      options.push(new Option(caption ? `${dependency.key} · ${caption}` : dependency.key, dependency.key, false, Boolean(edge)))
    }
    depends.replaceChildren(...options)
  }

  function refresh(selected = -1) {
    try {
      const definition = readDefinition()
      nodeList.replaceChildren(new Option('新增节点', '-1'), ...definition.nodes.map((node, index) => new Option(node.key, String(index))))
      nodeList.value = String(selected)
      fillNode(definition, selected)
      graphEditor.hidden = false
    } catch {
      graphEditor.hidden = true
    }
  }

  source.addEventListener('change', () => refresh())
  nodeList.addEventListener('change', () => fillNode(readDefinition(), Number(nodeList.value)))
  editor.querySelector('[data-import]').addEventListener('change', async event => {
    const file = event.target.files[0]
    if (!file) return
    if (file.size > maxBytes) {
      message.textContent = '定义文件不能超过 1 MiB。'
      return
    }
    try {
      source.value = await file.text()
      refresh()
      message.textContent = '定义已导入，请提交校验。'
    } catch {
      message.textContent = '无法读取定义文件。'
    }
  })

  editor.querySelector('[data-download]').addEventListener('click', () => {
    try {
      const definition = readDefinition()
      if (!source.value.trim()) throw new Error('请先创建或导入定义。')
      const file = new Blob([source.value], { type: 'application/json' })
      const url = URL.createObjectURL(file)
      const link = document.createElement('a')
      link.href = url
      link.download = /^[a-z][a-z0-9_-]{0,63}$/.test(definition.key) ? `${definition.key}.json` : 'workflow.json'
      link.click()
      setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (error) {
      message.textContent = error.message
    }
  })

  editor.querySelector('[data-apply-node]').addEventListener('click', () => {
    try {
      const definition = readDefinition()
      const selected = Number(nodeList.value)
      const old = definition.nodes[selected]
      const key = nodeKey.value.trim()
      const identifiers = [workflowKey.value.trim(), key, task.value.trim(), queue.value.trim()]
      const seconds = Number(timeout.value)
      if (identifiers.some(value => !/^[a-z][a-z0-9_-]{0,63}$/.test(value)) || !Number.isInteger(seconds) || seconds < 1) {
        throw new Error('请输入有效的工作流键、节点键、任务类型、队列和超时秒数。')
      }
      if (definition.nodes.some((node, index) => node.key === key && selected !== index)) {
        throw new Error('节点键已存在。')
      }
      const edges = new Map((old?.depends_on ?? []).map(edge => [edge.node, edge]))
      const node = { retry: { max_attempts: 1 }, effect: 'none', ...old,
        key, task: identifiers[2], queue: identifiers[3], timeout_seconds: seconds,
        depends_on: Array.from(depends.selectedOptions, option => edges.get(option.value) ?? { node: option.value, when: 'succeeded' }) }
      definition.key = identifiers[0]
      if (old) {
        definition.nodes[selected] = node
        for (const next of definition.nodes) {
          for (const edge of next.depends_on ?? []) if (edge.node === old.key) edge.node = key
          for (const input of Object.values(next.input ?? {})) if (input.node === old.key) input.node = key
        }
      } else {
        definition.nodes.push(node)
      }
      source.value = JSON.stringify(definition, null, 2)
      refresh(old ? selected : definition.nodes.length - 1)
      message.textContent = '定义已更新，请校验并预览。'
    } catch (error) {
      message.textContent = error.message
    }
  })
  refresh()
}

if (location.hash.startsWith('#node-')) {
  const node = document.getElementById(location.hash.slice(1))
  if (node instanceof HTMLDetailsElement) node.open = true
}
