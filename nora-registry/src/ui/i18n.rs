// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

/// Internationalization support for the UI
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lang {
    #[default]
    En,
    Ru,
    Zh,
}

impl Lang {
    pub fn from_str(s: &str) -> Self {
        let lower = s.to_lowercase();
        // Match on the primary language subtag, ignoring any region/script
        // suffix so BCP-47 / POSIX tags ("zh-CN", "zh-Hans", "ru_RU.UTF-8")
        // resolve the same as their bare code. Simplified Chinese is the only
        // Chinese variant available, so every "zh*" tag maps to it.
        let primary = lower
            .split(['-', '_'])
            .next()
            .expect("str::split always yields at least one segment");
        match primary {
            "ru" | "rus" | "russian" => Lang::Ru,
            "zh" | "zho" | "chs" | "chinese" => Lang::Zh,
            _ => Lang::En,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Lang::En => "en",
            Lang::Ru => "ru",
            Lang::Zh => "zh",
        }
    }
}

/// All translatable strings
#[allow(dead_code)]
pub struct Translations {
    // Navigation
    pub nav_dashboard: &'static str,
    pub nav_registries: &'static str,
    pub open_navigation_menu: &'static str,
    pub close_navigation_menu: &'static str,
    pub primary_navigation: &'static str,
    pub language_selector: &'static str,
    pub github_link: &'static str,
    pub api_documentation: &'static str,
    pub skip_to_content: &'static str,

    // Dashboard
    pub dashboard_title: &'static str,
    pub dashboard_subtitle: &'static str,
    pub uptime: &'static str,

    // Stats
    pub stat_downloads: &'static str,
    pub stat_uploads: &'static str,
    pub stat_artifacts: &'static str,
    pub stat_cache_hit: &'static str,
    pub stats_since_restart: &'static str,

    // Registry cards
    pub active: &'static str,
    pub artifacts: &'static str,
    pub size: &'static str,
    pub downloads: &'static str,
    pub uploads: &'static str,

    // Mount points
    pub mount_points: &'static str,
    pub registry: &'static str,
    pub mount_path: &'static str,
    pub proxy_upstream: &'static str,

    // Activity
    pub recent_activity: &'static str,
    pub last_n_events: &'static str,
    pub time: &'static str,
    pub action: &'static str,
    pub artifact: &'static str,
    pub source: &'static str,
    pub no_activity: &'static str,

    // Relative time
    pub just_now: &'static str,
    pub min_ago: &'static str,
    pub mins_ago: &'static str,
    pub hour_ago: &'static str,
    pub hours_ago: &'static str,
    pub day_ago: &'static str,
    pub days_ago: &'static str,

    // Registry pages
    pub repositories: &'static str,
    pub search_placeholder: &'static str,
    pub search_maven: &'static str,
    pub search_packages: &'static str,
    pub no_repos_found: &'static str,
    pub no_versions_found: &'static str,
    pub no_search_results: &'static str,
    pub one_result_on_page: &'static str,
    pub results_on_page: &'static str,
    pub one_result_for_query: &'static str,
    pub results_for_query: &'static str,
    pub next_results: &'static str,
    pub search_results_changed: &'static str,
    pub search_temporarily_unavailable: &'static str,
    pub reload_results: &'static str,
    pub index_view_temporarily_unavailable: &'static str,
    pub reload_page: &'static str,
    pub push_first_artifact: &'static str,
    pub name: &'static str,
    pub tags: &'static str,
    pub versions: &'static str,
    pub updated: &'static str,

    // Persistent index loading state
    pub index_loading_title: &'static str,
    pub index_loading_message: &'static str,
    pub index_loading_hint: &'static str,
    pub index_loading_no_js: &'static str,
    pub index_progress_phase: &'static str,
    pub index_phase_idle: &'static str,
    pub index_phase_preparing: &'static str,
    pub index_phase_recovering: &'static str,
    pub index_phase_maven_inventory: &'static str,
    pub index_phase_npm_inventory: &'static str,
    pub index_phase_npm_authority: &'static str,
    pub index_phase_publishing: &'static str,
    pub index_phase_retry_waiting: &'static str,
    pub index_progress_maven_objects: &'static str,
    pub index_progress_npm_objects: &'static str,
    pub index_progress_npm_packages: &'static str,
    pub index_progress_commit_hint: &'static str,

    // Detail pages
    pub pull_command: &'static str,
    pub install_command: &'static str,
    pub maven_dependency: &'static str,
    pub maven_artifacts: &'static str,
    pub no_artifacts_found: &'static str,
    pub next_files: &'static str,
    pub total: &'static str,
    pub created: &'static str,
    pub published: &'static str,
    pub filename: &'static str,
    pub files: &'static str,
    pub prerelease_versions: &'static str,
    pub stable_versions: &'static str,
    pub cached: &'static str,
    pub next_versions: &'static str,
    pub show_all_stable_versions: &'static str,
    pub copy_to_clipboard: &'static str,
    pub copied: &'static str,
    pub copy_failed: &'static str,
    pub homepage: &'static str,
    pub repository_label: &'static str,
    pub license_label: &'static str,
    pub author_label: &'static str,
    pub package_description: &'static str,

    // Bragging footer
    pub built_for_speed: &'static str,
    pub docker_image: &'static str,
    pub cold_start: &'static str,
    pub memory: &'static str,
    pub registries_count: &'static str,
    pub multi_arch: &'static str,
    pub zero_deps: &'static str,
    pub deps_label: &'static str,
    pub tagline: &'static str,

    // Token management
    pub nav_admin: &'static str,
    pub nav_tokens: &'static str,
    pub token_management: &'static str,
    pub token_management_subtitle: &'static str,
    pub token_create: &'static str,
    pub token_revoke: &'static str,
    pub token_revoke_confirm: &'static str,
    pub token_description: &'static str,
    pub token_description_placeholder: &'static str,
    pub token_role: &'static str,
    pub token_ttl: &'static str,
    pub token_ttl_days: &'static str,
    pub token_created_success: &'static str,
    pub token_created_warning: &'static str,
    pub token_copy: &'static str,
    pub token_no_tokens: &'static str,
    pub token_user: &'static str,
    pub token_expires: &'static str,
    pub token_last_used: &'static str,
    pub token_never_used: &'static str,

    // Pagination
    pub showing_range: &'static str,
    pub showing_all: &'static str,
    pub no_more_items: &'static str,
    pub one_file: &'static str,
    pub items: &'static str,
    pub maven_items: &'static str,
}

pub fn get_translations(lang: Lang) -> &'static Translations {
    match lang {
        Lang::En => &TRANSLATIONS_EN,
        Lang::Ru => &TRANSLATIONS_RU,
        Lang::Zh => &TRANSLATIONS_ZH,
    }
}

pub static TRANSLATIONS_EN: Translations = Translations {
    // Navigation
    nav_dashboard: "Dashboard",
    nav_registries: "Registries",
    open_navigation_menu: "Open navigation menu",
    close_navigation_menu: "Close navigation menu",
    primary_navigation: "Primary navigation",
    language_selector: "Language",
    github_link: "NORA on GitHub",
    api_documentation: "API documentation",
    skip_to_content: "Skip to main content",

    // Dashboard
    dashboard_title: "Dashboard",
    dashboard_subtitle: "Overview of all registries",
    uptime: "Uptime",

    // Stats
    stat_downloads: "Downloads",
    stat_uploads: "Uploads",
    stat_artifacts: "Artifacts",
    stat_cache_hit: "Cache Hit",
    stats_since_restart: "since restart",

    // Registry cards
    active: "ACTIVE",
    artifacts: "Artifacts",
    size: "Size",
    downloads: "Downloads",
    uploads: "Uploads",

    // Mount points
    mount_points: "Mount Points",
    registry: "Registry",
    mount_path: "Mount Path",
    proxy_upstream: "Proxy Upstream",

    // Activity
    recent_activity: "Recent Activity",
    last_n_events: "Last 20 events",
    time: "Time",
    action: "Action",
    artifact: "Artifact",
    source: "Source",
    no_activity: "No recent activity",

    // Relative time
    just_now: "just now",
    min_ago: "min ago",
    mins_ago: "mins ago",
    hour_ago: "hour ago",
    hours_ago: "hours ago",
    day_ago: "day ago",
    days_ago: "days ago",

    // Registry pages
    repositories: "repositories",
    search_placeholder: "Search repositories...",
    search_maven: "Search all indexed Maven paths...",
    search_packages: "Search packages...",
    no_repos_found: "No repositories found",
    no_versions_found: "No versions in this view",
    no_search_results: "No matching results",
    one_result_on_page: "1 result on this page",
    results_on_page: "{count} results on this page",
    one_result_for_query: "1 result for “{query}” on this page",
    results_for_query: "{count} results for “{query}” on this page",
    next_results: "Next results",
    search_results_changed: "The index changed while you were browsing. Reload current results.",
    search_temporarily_unavailable: "Search is temporarily unavailable while the index recovers.",
    reload_results: "Reload results",
    index_view_temporarily_unavailable:
        "This repository view is temporarily unavailable while the local index recovers.",
    reload_page: "Reload page",
    push_first_artifact: "Push your first artifact to see it here",
    name: "Name",
    tags: "Tags",
    versions: "Versions",
    updated: "Updated",

    // Persistent index loading state
    index_loading_title: "Preparing the {registry} index",
    index_loading_message:
        "NORA is building a local view of the artifacts. This page will refresh automatically.",
    index_loading_hint: "Repository API operations remain available while indexing runs.",
    index_loading_no_js: "JavaScript is disabled. Reload this page to check again.",
    index_progress_phase: "Current phase",
    index_phase_idle: "Waiting for the index worker",
    index_phase_preparing: "Preparing the local index",
    index_phase_recovering: "Recovering the local index",
    index_phase_maven_inventory: "Reading the Maven object inventory",
    index_phase_npm_inventory: "Reading the npm object inventory",
    index_phase_npm_authority: "Validating npm package metadata",
    index_phase_publishing: "Publishing the new index generation",
    index_phase_retry_waiting: "Temporary failure; retry is scheduled",
    index_progress_maven_objects: "Maven objects indexed",
    index_progress_npm_objects: "npm objects indexed",
    index_progress_npm_packages: "npm packages processed",
    index_progress_commit_hint: "Counts update after each durable index batch.",

    // Detail pages
    pull_command: "Pull Command",
    install_command: "Install Command",
    maven_dependency: "Maven Dependency",
    maven_artifacts: "Maven artifacts",
    no_artifacts_found: "No artifacts found",
    next_files: "Next files",
    total: "total",
    created: "Created",
    published: "Published",
    filename: "Filename",
    files: "files",
    prerelease_versions: "pre-release versions",
    stable_versions: "Show stable versions",
    cached: "cached",
    next_versions: "Next versions",
    show_all_stable_versions: "Show all {count} stable versions",
    copy_to_clipboard: "Copy to clipboard",
    copied: "Copied",
    copy_failed: "Copy failed. Select the command and copy it manually.",
    homepage: "Homepage",
    repository_label: "Repository",
    license_label: "License",
    author_label: "Author",
    package_description: "Package description",

    // Bragging footer
    built_for_speed: "Built for speed",
    docker_image: "Docker Image",
    cold_start: "Cold Start",
    memory: "Memory",
    registries_count: "Registries",
    multi_arch: "Multi-arch",
    zero_deps: "0",
    deps_label: "Dependencies",
    tagline: "Pure Rust. Single binary. OCI compatible.",

    // Token management
    nav_admin: "Admin",
    nav_tokens: "Tokens",
    token_management: "Token Management",
    token_management_subtitle: "Create and manage API tokens for programmatic access",
    token_create: "Create Token",
    token_revoke: "Revoke",
    token_revoke_confirm: "Are you sure you want to revoke this token?",
    token_description: "Description",
    token_description_placeholder: "e.g. CI/CD Pipeline",
    token_role: "Role",
    token_ttl: "TTL",
    token_ttl_days: "days",
    token_created_success: "Token created successfully. Copy it now — it won't be shown again.",
    token_created_warning: "This token will not be displayed again. Store it securely.",
    token_copy: "Copy",
    token_no_tokens: "No tokens yet. Create one to get started.",
    token_user: "User",
    token_expires: "Expires",
    token_last_used: "Last Used",
    token_never_used: "Never",

    // Pagination
    showing_range: "Showing {start}-{end} of {total} items",
    showing_all: "Showing all {count} items",
    no_more_items: "No more items on this page",
    one_file: "1 file",
    items: "Files",
    maven_items: "Items",
};

pub static TRANSLATIONS_RU: Translations = Translations {
    // Navigation
    nav_dashboard: "Панель",
    nav_registries: "Реестры",
    open_navigation_menu: "Открыть меню навигации",
    close_navigation_menu: "Закрыть меню навигации",
    primary_navigation: "Основная навигация",
    language_selector: "Язык",
    github_link: "NORA на GitHub",
    api_documentation: "Документация API",
    skip_to_content: "Перейти к основному содержимому",

    // Dashboard
    dashboard_title: "Панель управления",
    dashboard_subtitle: "Обзор всех реестров",
    uptime: "Время работы",

    // Stats
    stat_downloads: "Загрузки",
    stat_uploads: "Публикации",
    stat_artifacts: "Артефакты",
    stat_cache_hit: "Кэш",
    stats_since_restart: "с момента перезапуска",

    // Registry cards
    active: "АКТИВЕН",
    artifacts: "Артефакты",
    size: "Размер",
    downloads: "Загрузки",
    uploads: "Публикации",

    // Mount points
    mount_points: "Точки монтирования",
    registry: "Реестр",
    mount_path: "Путь",
    proxy_upstream: "Прокси",

    // Activity
    recent_activity: "Последняя активность",
    last_n_events: "Последние 20 событий",
    time: "Время",
    action: "Действие",
    artifact: "Артефакт",
    source: "Источник",
    no_activity: "Нет активности",

    // Relative time
    just_now: "только что",
    min_ago: "мин назад",
    mins_ago: "мин назад",
    hour_ago: "час назад",
    hours_ago: "ч назад",
    day_ago: "день назад",
    days_ago: "дн назад",

    // Registry pages
    repositories: "репозиториев",
    search_placeholder: "Поиск репозиториев...",
    search_maven: "Поиск по всем проиндексированным Maven-путям...",
    search_packages: "Поиск пакетов...",
    no_repos_found: "Репозитории не найдены",
    no_versions_found: "В этом режиме версий нет",
    no_search_results: "Совпадения не найдены",
    one_result_on_page: "1 результат на этой странице",
    results_on_page: "Результатов на странице: {count}",
    one_result_for_query: "1 результат по запросу «{query}» на этой странице",
    results_for_query: "Результатов по запросу «{query}» на этой странице: {count}",
    next_results: "Следующие результаты",
    search_results_changed: "Индекс обновился во время просмотра. Загрузите актуальные результаты.",
    search_temporarily_unavailable: "Поиск временно недоступен, пока индекс восстанавливается.",
    reload_results: "Загрузить результаты",
    index_view_temporarily_unavailable:
        "Этот экран репозитория временно недоступен, пока локальный индекс восстанавливается.",
    reload_page: "Обновить страницу",
    push_first_artifact: "Загрузите первый артефакт, чтобы увидеть его здесь",
    name: "Название",
    tags: "Теги",
    versions: "Версии",
    updated: "Обновлено",

    // Persistent index loading state
    index_loading_title: "Подготавливаем индекс {registry}",
    index_loading_message:
        "NORA строит локальное представление артефактов. Страница обновится автоматически.",
    index_loading_hint: "API репозиториев продолжает работать во время индексации.",
    index_loading_no_js: "JavaScript отключён. Обновите страницу, чтобы проверить снова.",
    index_progress_phase: "Текущий этап",
    index_phase_idle: "Ожидание запуска индексатора",
    index_phase_preparing: "Подготовка локального индекса",
    index_phase_recovering: "Восстановление локального индекса",
    index_phase_maven_inventory: "Чтение списка объектов Maven",
    index_phase_npm_inventory: "Чтение списка объектов npm",
    index_phase_npm_authority: "Проверка метаданных пакетов npm",
    index_phase_publishing: "Публикация нового поколения индекса",
    index_phase_retry_waiting: "Временная ошибка — ожидаем повтор",
    index_progress_maven_objects: "Объектов Maven проиндексировано",
    index_progress_npm_objects: "Объектов npm проиндексировано",
    index_progress_npm_packages: "Пакетов npm обработано",
    index_progress_commit_hint: "Счётчики обновляются после каждой сохранённой порции.",

    // Detail pages
    pull_command: "Команда загрузки",
    install_command: "Команда установки",
    maven_dependency: "Maven зависимость",
    maven_artifacts: "Артефакты Maven",
    no_artifacts_found: "Артефакты не найдены",
    next_files: "Следующие файлы",
    total: "всего",
    created: "Создан",
    published: "Опубликован",
    filename: "Файл",
    files: "файлов",
    prerelease_versions: "предварительных версий",
    stable_versions: "Показать стабильные версии",
    cached: "в кэше",
    next_versions: "Следующие версии",
    show_all_stable_versions: "Показать все стабильные версии ({count})",
    copy_to_clipboard: "Копировать в буфер обмена",
    copied: "Скопировано",
    copy_failed: "Не удалось скопировать. Выделите и скопируйте команду вручную.",
    homepage: "Домашняя страница",
    repository_label: "Репозиторий",
    license_label: "Лицензия",
    author_label: "Автор",
    package_description: "Описание пакета",

    // Bragging footer
    built_for_speed: "Создан для скорости",
    docker_image: "Docker образ",
    cold_start: "Холодный старт",
    memory: "Память",
    registries_count: "Реестров",
    multi_arch: "Мультиарх",
    zero_deps: "0",
    deps_label: "зависимостей",
    tagline: "Чистый Rust. Один бинарник. OCI совместимый.",

    // Token management
    nav_admin: "Управление",
    nav_tokens: "Токены",
    token_management: "Управление токенами",
    token_management_subtitle: "Создание и управление API-токенами для программного доступа",
    token_create: "Создать токен",
    token_revoke: "Отозвать",
    token_revoke_confirm: "Вы уверены, что хотите отозвать этот токен?",
    token_description: "Описание",
    token_description_placeholder: "напр. CI/CD Pipeline",
    token_role: "Роль",
    token_ttl: "Срок",
    token_ttl_days: "дней",
    token_created_success: "Токен создан. Скопируйте его сейчас — он больше не будет показан.",
    token_created_warning: "Этот токен не будет показан повторно. Сохраните его надёжно.",
    token_copy: "Копировать",
    token_no_tokens: "Нет токенов. Создайте первый для начала работы.",
    token_user: "Пользователь",
    token_expires: "Истекает",
    token_last_used: "Последнее использование",
    token_never_used: "Не использовался",

    // Pagination
    showing_range: "Показаны {start}-{end} из {total}",
    showing_all: "Показаны все ({count})",
    no_more_items: "На этой странице больше нет элементов",
    one_file: "1 файл",
    items: "Файлы",
    maven_items: "Элементы",
};

pub static TRANSLATIONS_ZH: Translations = Translations {
    // Navigation
    nav_dashboard: "仪表盘",
    nav_registries: "注册表",
    open_navigation_menu: "打开导航菜单",
    close_navigation_menu: "关闭导航菜单",
    primary_navigation: "主导航",
    language_selector: "语言",
    github_link: "GitHub 上的 NORA",
    api_documentation: "API 文档",
    skip_to_content: "跳到主要内容",

    // Dashboard
    dashboard_title: "仪表盘",
    dashboard_subtitle: "所有注册表概览",
    uptime: "运行时间",

    // Stats
    stat_downloads: "下载量",
    stat_uploads: "上传量",
    stat_artifacts: "制品数",
    stat_cache_hit: "缓存命中",
    stats_since_restart: "自重启以来",

    // Registry cards
    active: "活跃",
    artifacts: "制品",
    size: "大小",
    downloads: "下载",
    uploads: "上传",

    // Mount points
    mount_points: "挂载点",
    registry: "注册表",
    mount_path: "挂载路径",
    proxy_upstream: "代理上游",

    // Activity
    recent_activity: "最近活动",
    last_n_events: "最近 20 条事件",
    time: "时间",
    action: "操作",
    artifact: "制品",
    source: "来源",
    no_activity: "暂无最近活动",

    // Relative time
    just_now: "刚刚",
    min_ago: "分钟前",
    mins_ago: "分钟前",
    hour_ago: "小时前",
    hours_ago: "小时前",
    day_ago: "天前",
    days_ago: "天前",

    // Registry pages
    repositories: "个仓库",
    search_placeholder: "搜索仓库...",
    search_maven: "搜索所有已索引的 Maven 路径...",
    search_packages: "搜索软件包...",
    no_repos_found: "未找到仓库",
    no_versions_found: "当前视图中没有版本",
    no_search_results: "未找到匹配结果",
    one_result_on_page: "本页有 1 个结果",
    results_on_page: "本页 {count} 个结果",
    one_result_for_query: "本页有 1 个与“{query}”匹配的结果",
    results_for_query: "本页有 {count} 个与“{query}”匹配的结果",
    next_results: "下一页结果",
    search_results_changed: "浏览期间索引已更新。请重新加载当前结果。",
    search_temporarily_unavailable: "索引恢复期间搜索暂时不可用。",
    reload_results: "重新加载结果",
    index_view_temporarily_unavailable: "本地索引恢复期间，此仓库视图暂时不可用。",
    reload_page: "重新加载页面",
    push_first_artifact: "推送您的第一个制品即可在此查看",
    name: "名称",
    tags: "标签",
    versions: "版本",
    updated: "更新于",

    // Persistent index loading state
    index_loading_title: "正在准备 {registry} 索引",
    index_loading_message: "NORA 正在构建制品的本地视图。此页面将自动刷新。",
    index_loading_hint: "索引期间，仓库 API 仍可继续使用。",
    index_loading_no_js: "JavaScript 已禁用。请刷新页面以再次检查。",
    index_progress_phase: "当前阶段",
    index_phase_idle: "等待索引任务",
    index_phase_preparing: "准备本地索引",
    index_phase_recovering: "恢复本地索引",
    index_phase_maven_inventory: "读取 Maven 对象清单",
    index_phase_npm_inventory: "读取 npm 对象清单",
    index_phase_npm_authority: "验证 npm 包元数据",
    index_phase_publishing: "发布新索引版本",
    index_phase_retry_waiting: "暂时失败，等待重试",
    index_progress_maven_objects: "已索引 Maven 对象",
    index_progress_npm_objects: "已索引 npm 对象",
    index_progress_npm_packages: "已处理 npm 包",
    index_progress_commit_hint: "计数会在每批数据持久化后更新。",

    // Detail pages
    pull_command: "拉取命令",
    install_command: "安装命令",
    maven_dependency: "Maven 依赖",
    maven_artifacts: "Maven 构件",
    no_artifacts_found: "未找到构件",
    next_files: "下一批文件",
    total: "共",
    created: "创建于",
    published: "发布于",
    filename: "文件名",
    files: "个文件",
    prerelease_versions: "个预发布版本",
    stable_versions: "显示稳定版本",
    cached: "已缓存",
    next_versions: "下一页版本",
    show_all_stable_versions: "显示全部 {count} 个稳定版本",
    copy_to_clipboard: "复制到剪贴板",
    copied: "已复制",
    copy_failed: "复制失败。请手动选择并复制命令。",
    homepage: "主页",
    repository_label: "代码仓库",
    license_label: "许可证",
    author_label: "作者",
    package_description: "软件包描述",

    // Bragging footer
    built_for_speed: "为速度而生",
    docker_image: "Docker 镜像",
    cold_start: "冷启动",
    memory: "内存",
    registries_count: "注册表",
    multi_arch: "多架构",
    zero_deps: "0",
    deps_label: "依赖",
    tagline: "纯 Rust 实现。单二进制文件。兼容 OCI。",

    // Token management
    nav_admin: "管理",
    nav_tokens: "令牌",
    token_management: "令牌管理",
    token_management_subtitle: "创建和管理用于程序化访问的 API 令牌",
    token_create: "创建令牌",
    token_revoke: "撤销",
    token_revoke_confirm: "确定要撤销此令牌吗？",
    token_description: "描述",
    token_description_placeholder: "例如 CI/CD 流水线",
    token_role: "角色",
    token_ttl: "有效期",
    token_ttl_days: "天",
    token_created_success: "令牌创建成功。请立即复制 — 它将不再显示。",
    token_created_warning: "此令牌将不再显示。请妥善保管。",
    token_copy: "复制",
    token_no_tokens: "暂无令牌。创建一个即可开始使用。",
    token_user: "用户",
    token_expires: "过期时间",
    token_last_used: "最后使用",
    token_never_used: "从未使用",

    // Pagination
    showing_range: "显示第 {start}-{end} 项，共 {total} 项",
    showing_all: "显示全部 {count} 项",
    no_more_items: "此页没有更多项目了",
    one_file: "1 个文件",
    items: "文件",
    maven_items: "项目",
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_resolves_bare_codes() {
        assert_eq!(Lang::from_str("en"), Lang::En);
        assert_eq!(Lang::from_str("ru"), Lang::Ru);
        assert_eq!(Lang::from_str("zh"), Lang::Zh);
        assert_eq!(Lang::from_str("chinese"), Lang::Zh);
    }

    #[test]
    fn from_str_ignores_region_and_script_subtags() {
        // BCP-47 region/script tags resolve to the primary language.
        assert_eq!(Lang::from_str("zh-CN"), Lang::Zh);
        assert_eq!(Lang::from_str("zh-Hans"), Lang::Zh);
        assert_eq!(Lang::from_str("zh-TW"), Lang::Zh); // only Simplified available
        assert_eq!(Lang::from_str("ru-RU"), Lang::Ru);
        assert_eq!(Lang::from_str("ru_RU.UTF-8"), Lang::Ru);
        assert_eq!(Lang::from_str("en-US"), Lang::En);
    }

    #[test]
    fn from_str_defaults_to_english_for_unknown() {
        assert_eq!(Lang::from_str("fr"), Lang::En);
        assert_eq!(Lang::from_str(""), Lang::En);
    }

    #[test]
    fn code_round_trips_through_from_str() {
        for lang in [Lang::En, Lang::Ru, Lang::Zh] {
            assert_eq!(Lang::from_str(lang.code()), lang);
        }
    }
}
