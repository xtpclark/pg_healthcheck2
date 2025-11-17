/**
 * Dashboard Table - Modern table-based dashboard replacing timeline
 * Features: Sorting, filtering, pagination, bulk actions, filter persistence
 */

// State management
const state = {
    runs: [],
    filteredRuns: [],
    selectedRunIds: new Set(),
    currentPage: 1,
    pageSize: 50,
    sortColumn: 'timestamp',
    sortDirection: 'desc',
    showDeleted: false,  // Toggle for showing deleted runs
    filters: {
        company: '',
        target: '',
        timerange: '30d',
        technology: '',
        status: '',
        search: '',
        startDate: '',
        endDate: ''
    }
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', async () => {
    setupEventListeners();
    await loadSelectedFilter();
    await fetchRuns();
});

// Event Listeners
function setupEventListeners() {
    // Filter changes
    document.getElementById('company-filter').addEventListener('change', (e) => {
        state.filters.company = e.target.value;

        // Reset target filter when company changes
        state.filters.target = '';

        // Update target dropdown to show only targets for selected company
        populateTargetFilter();

        applyFilters();
    });

    document.getElementById('target-filter').addEventListener('change', (e) => {
        state.filters.target = e.target.value;
        applyFilters();
    });

    document.getElementById('timerange-filter').addEventListener('change', (e) => {
        state.filters.timerange = e.target.value;
        if (e.target.value === 'custom') {
            document.getElementById('custom-dates').classList.remove('hidden');
        } else {
            document.getElementById('custom-dates').classList.add('hidden');
        }
        applyFilters();
    });

    document.getElementById('technology-filter').addEventListener('change', (e) => {
        state.filters.technology = e.target.value;
        applyFilters();
    });

    document.getElementById('status-filter').addEventListener('change', (e) => {
        state.filters.status = e.target.value;
        applyFilters();
    });

    document.getElementById('search-filter').addEventListener('input', debounce((e) => {
        state.filters.search = e.target.value;
        applyFilters();
    }, 300));

    document.getElementById('start-date').addEventListener('change', (e) => {
        state.filters.startDate = e.target.value;
        applyFilters();
    });

    document.getElementById('end-date').addEventListener('change', (e) => {
        state.filters.endDate = e.target.value;
        applyFilters();
    });

    // Select all checkbox
    document.getElementById('select-all').addEventListener('change', (e) => {
        const visibleRunIds = state.filteredRuns.slice(
            (state.currentPage - 1) * state.pageSize,
            state.currentPage * state.pageSize
        ).map(run => run.id);

        if (e.target.checked) {
            visibleRunIds.forEach(id => state.selectedRunIds.add(id));
        } else {
            visibleRunIds.forEach(id => state.selectedRunIds.delete(id));
        }
        updateTableSelection();
        updateActionBar();
    });

    // Action bar buttons
    document.getElementById('compare-selected-btn').addEventListener('click', handleCompareSelected);

    const generateAiBtn = document.getElementById('generate-ai-btn');
    if (generateAiBtn) {
        generateAiBtn.addEventListener('click', handleGenerateAiReport);
    }

    const bulkDeleteBtn = document.getElementById('bulk-delete-btn');
    if (bulkDeleteBtn) {
        bulkDeleteBtn.addEventListener('click', handleBulkDelete);
    }

    const toggleDeletedBtn = document.getElementById('toggle-deleted-btn');
    if (toggleDeletedBtn) {
        toggleDeletedBtn.addEventListener('click', handleToggleDeleted);
    }

    document.getElementById('clear-selection-btn').addEventListener('click', () => {
        state.selectedRunIds.clear();
        updateTableSelection();
        updateActionBar();
    });

    // Filter persistence
    document.getElementById('save-filter-btn').addEventListener('click', () => {
        document.getElementById('save-filter-modal').classList.remove('hidden');
        document.getElementById('save-filter-modal').classList.add('flex');
    });

    document.getElementById('confirm-save-filter-btn').addEventListener('click', handleSaveFilter);

    document.getElementById('load-filter-btn').addEventListener('click', handleLoadFilter);

    // Sortable column headers
    document.querySelectorAll('th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const column = th.dataset.sort;
            if (state.sortColumn === column) {
                state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                state.sortColumn = column;
                state.sortDirection = 'desc';
            }
            updateSortIndicators();
            applyFilters();
        });
    });
}

// Data Fetching
async function fetchRuns() {
    try {
        // Build query string with show deleted parameter
        const params = new URLSearchParams();
        if (state.showDeleted) {
            params.set('include_deleted', 'true');
        }

        const url = '/api/runs' + (params.toString() ? '?' + params.toString() : '');
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch runs');

        const data = await response.json();
        state.runs = data.runs || [];

        // Populate filter dropdowns
        populateCompanyFilter();
        populateTargetFilter();

        applyFilters();
    } catch (error) {
        console.error('Error fetching runs:', error);
        showError('Failed to load health check runs');
    }
}

// Populate company filter with unique companies
function populateCompanyFilter() {
    const companies = new Set();
    state.runs.forEach(run => {
        if (run.company_name) {
            companies.add(run.company_name);
        }
    });

    const companyFilter = document.getElementById('company-filter');
    const currentValue = companyFilter.value;

    // Clear existing options except "All Companies"
    companyFilter.innerHTML = '<option value="">All Companies</option>';

    // Add sorted company options
    Array.from(companies).sort().forEach(company => {
        const option = document.createElement('option');
        option.value = company;
        option.textContent = company;
        companyFilter.appendChild(option);
    });

    // Restore previous selection if still valid
    if (currentValue && companies.has(currentValue)) {
        companyFilter.value = currentValue;
    }
}

// Populate target filter based on selected company (cascading filter)
function populateTargetFilter() {
    const targets = new Set();

    // Filter targets by selected company
    state.runs.forEach(run => {
        if (run.target_system) {
            // If company filter is set, only show targets for that company
            if (!state.filters.company || run.company_name === state.filters.company) {
                targets.add(run.target_system);
            }
        }
    });

    const targetFilter = document.getElementById('target-filter');
    const currentValue = targetFilter.value;

    // Clear existing options
    targetFilter.innerHTML = '<option value="">All Targets</option>';

    // Add sorted target options
    Array.from(targets).sort().forEach(target => {
        const option = document.createElement('option');
        option.value = target;
        option.textContent = target;
        targetFilter.appendChild(option);
    });

    // Restore previous selection if still valid
    if (currentValue && targets.has(currentValue)) {
        targetFilter.value = currentValue;
    } else {
        // If previous selection is not valid, reset
        targetFilter.value = '';
    }
}

// Filter Application
function applyFilters() {
    let filtered = [...state.runs];

    // Company filter
    if (state.filters.company) {
        filtered = filtered.filter(run => run.company_name === state.filters.company);
    }

    // Target filter
    if (state.filters.target) {
        filtered = filtered.filter(run => run.target_system === state.filters.target);
    }

    // Technology filter
    if (state.filters.technology) {
        filtered = filtered.filter(run =>
            run.db_technology.toLowerCase() === state.filters.technology.toLowerCase()
        );
    }

    // Status filter
    if (state.filters.status) {
        switch (state.filters.status) {
            case 'favorites':
                filtered = filtered.filter(run => run.is_favorite);
                break;
            case 'critical':
                filtered = filtered.filter(run => (run.critical_count || 0) > 0);
                break;
            case 'warnings':
                filtered = filtered.filter(run => (run.high_count || 0) > 0 || (run.medium_count || 0) > 0);
                break;
            case 'healthy':
                filtered = filtered.filter(run =>
                    (run.critical_count || 0) === 0 &&
                    (run.high_count || 0) === 0 &&
                    (run.medium_count || 0) === 0
                );
                break;
        }
    }

    // Time range filter
    const now = new Date();
    if (state.filters.timerange !== 'custom') {
        const days = parseInt(state.filters.timerange);
        const cutoff = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
        filtered = filtered.filter(run => new Date(run.timestamp) >= cutoff);
    } else if (state.filters.startDate || state.filters.endDate) {
        if (state.filters.startDate) {
            const start = new Date(state.filters.startDate);
            filtered = filtered.filter(run => new Date(run.timestamp) >= start);
        }
        if (state.filters.endDate) {
            const end = new Date(state.filters.endDate);
            end.setHours(23, 59, 59, 999); // End of day
            filtered = filtered.filter(run => new Date(run.timestamp) <= end);
        }
    }

    // Search filter
    if (state.filters.search) {
        const search = state.filters.search.toLowerCase();
        filtered = filtered.filter(run =>
            (run.company_name || '').toLowerCase().includes(search) ||
            (run.target_system || '').toLowerCase().includes(search) ||
            (run.db_technology || '').toLowerCase().includes(search)
        );
    }

    // Sort
    filtered.sort((a, b) => {
        let aVal, bVal;

        switch (state.sortColumn) {
            case 'timestamp':
                aVal = new Date(a.timestamp);
                bVal = new Date(b.timestamp);
                break;
            case 'company':
                aVal = (a.company_name || '').toLowerCase();
                bVal = (b.company_name || '').toLowerCase();
                break;
            case 'target':
                aVal = (a.target_system || '').toLowerCase();
                bVal = (b.target_system || '').toLowerCase();
                break;
            case 'technology':
                aVal = (a.db_technology || '').toLowerCase();
                bVal = (b.db_technology || '').toLowerCase();
                break;
            case 'status':
                aVal = (a.critical_count || 0) * 1000 + (a.high_count || 0) * 100 + (a.medium_count || 0);
                bVal = (b.critical_count || 0) * 1000 + (b.high_count || 0) * 100 + (b.medium_count || 0);
                break;
            default:
                aVal = a[state.sortColumn];
                bVal = b[state.sortColumn];
        }

        if (aVal < bVal) return state.sortDirection === 'asc' ? -1 : 1;
        if (aVal > bVal) return state.sortDirection === 'asc' ? 1 : -1;
        return 0;
    });

    state.filteredRuns = filtered;
    state.currentPage = 1; // Reset to first page
    updateStats();
    renderTable();
    renderPagination();
}

// Stats Update
function updateStats() {
    const total = state.filteredRuns.length;
    const healthy = state.filteredRuns.filter(run =>
        (run.critical_count || 0) === 0 &&
        (run.high_count || 0) === 0 &&
        (run.medium_count || 0) === 0
    ).length;
    const warnings = state.filteredRuns.filter(run =>
        (run.critical_count || 0) === 0 &&
        ((run.high_count || 0) > 0 || (run.medium_count || 0) > 0)
    ).length;
    const critical = state.filteredRuns.filter(run =>
        (run.critical_count || 0) > 0
    ).length;

    document.getElementById('stat-total').textContent = total;
    document.getElementById('stat-healthy').textContent = healthy;
    document.getElementById('stat-warnings').textContent = warnings;
    document.getElementById('stat-critical').textContent = critical;

    document.getElementById('visible-count').textContent = total;
}

// Helper: Check if user can edit based on technology
function canEditTechnology(technology) {
    if (!window.userPrivileges) return false;
    if (window.userPrivileges.canEdit) return true;

    const techMap = {
        'postgres': 'canEditPostgreSQL',
        'postgresql': 'canEditPostgreSQL',
        'kafka': 'canEditKafka',
        'cassandra': 'canEditCassandra',
        'opensearch': 'canEditOpenSearch',
        'clickhouse': 'canEditClickHouse',
        'mongodb': 'canEditMongoDB'
    };

    const privKey = techMap[technology?.toLowerCase()];
    return privKey && window.userPrivileges[privKey];
}

// Helper: Render edit button with privilege checking
function renderEditButton(run) {
    const canEdit = canEditTechnology(run.db_technology);

    if (canEdit) {
        return `<a href="/profile/edit-report/health_check/${run.id}"
                   class="inline-flex items-center px-2 py-1 text-xs border border-amber-500 text-amber-700 hover:bg-amber-50 transition-colors"
                   title="Edit Report">
                    <i class="bi bi-pencil-square"></i>
                </a>`;
    } else {
        return `<button class="inline-flex items-center px-2 py-1 text-xs border border-slate-300 text-slate-400 cursor-not-allowed"
                        disabled
                        title="Edit permission required">
                    <i class="bi bi-pencil-square"></i>
                </button>`;
    }
}

// Helper: Render download button with privilege checking
function renderDownloadButton(run) {
    if (window.userPrivileges && window.userPrivileges.canDownload) {
        return `<a href="/api/download-report/health_check/${run.id}"
                   class="inline-flex items-center px-2 py-1 text-xs border border-green-500 text-green-700 hover:bg-green-50 transition-colors"
                   title="Download Report"
                   download>
                    <i class="bi bi-download"></i>
                </a>`;
    } else {
        return `<button class="inline-flex items-center px-2 py-1 text-xs border border-slate-300 text-slate-400 cursor-not-allowed"
                        disabled
                        title="Download permission required">
                    <i class="bi bi-download"></i>
                </button>`;
    }
}

// Table Rendering
function renderTable() {
    const tbody = document.getElementById('runs-tbody');
    const start = (state.currentPage - 1) * state.pageSize;
    const end = start + state.pageSize;
    const pageRuns = state.filteredRuns.slice(start, end);

    if (pageRuns.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="8" class="px-4 py-8 text-center text-sm text-slate-500">
                    No runs found matching current filters
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = pageRuns.map(run => {
        const isSelected = state.selectedRunIds.has(run.id);
        const criticalCount = run.critical_count || 0;
        const highCount = run.high_count || 0;
        const mediumCount = run.medium_count || 0;

        let statusBadge, statusText;
        if (criticalCount > 0) {
            statusBadge = '<span class="px-2 py-1 text-xs font-medium bg-red-100 text-red-800 rounded">CRITICAL</span>';
            statusText = 'Critical Issues';
        } else if (highCount > 0) {
            statusBadge = '<span class="px-2 py-1 text-xs font-medium bg-orange-100 text-orange-800 rounded">WARNING</span>';
            statusText = 'Warnings';
        } else if (mediumCount > 0) {
            statusBadge = '<span class="px-2 py-1 text-xs font-medium bg-yellow-100 text-yellow-800 rounded">CAUTION</span>';
            statusText = 'Medium Issues';
        } else {
            statusBadge = '<span class="px-2 py-1 text-xs font-medium bg-green-100 text-green-800 rounded">HEALTHY</span>';
            statusText = 'Healthy';
        }

        const timestamp = new Date(run.timestamp).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });

        const rowClasses = [
            isSelected ? 'selected' : '',
            run.deleted_at ? 'bg-red-50 opacity-75' : 'hover:bg-slate-50',
            'transition-colors'
        ].filter(Boolean).join(' ');

        return `
            <tr class="${rowClasses}">
                <td class="px-4 py-3">
                    <input type="checkbox"
                           class="run-checkbox rounded border-slate-300"
                           data-run-id="${run.id}"
                           ${isSelected ? 'checked' : ''}
                           ${run.deleted_at ? 'disabled' : ''}>
                </td>
                <td class="px-4 py-3 text-sm text-slate-900">
                    ${timestamp}
                </td>
                <td class="px-4 py-3 text-sm font-medium text-slate-900">
                    ${escapeHtml(run.company_name || 'Unknown')}
                </td>
                <td class="px-4 py-3 text-sm text-slate-700 font-mono text-xs">
                    ${escapeHtml(run.target_system || 'N/A')}
                </td>
                <td class="px-4 py-3 text-sm text-slate-700 uppercase">
                    ${escapeHtml(run.db_technology || 'N/A')}
                </td>
                <td class="px-4 py-3 text-center">
                    ${statusBadge}
                </td>
                <td class="px-4 py-3 text-center text-sm text-slate-700">
                    ${criticalCount > 0 ? `<span class="text-red-700 font-medium">${criticalCount} critical</span>` : ''}
                    ${highCount > 0 ? `<span class="text-orange-700">${highCount} high</span>` : ''}
                    ${mediumCount > 0 ? `<span class="text-yellow-700">${mediumCount} med</span>` : ''}
                    ${criticalCount === 0 && highCount === 0 && mediumCount === 0 ? '<span class="text-green-700">None</span>' : ''}
                </td>
                <td class="px-4 py-3 text-center">
                    <div class="inline-flex items-center justify-center gap-1">
                        ${run.deleted_at ? `
                            <span class="text-xs text-red-700 font-medium mr-2">DELETED</span>
                            <button onclick="handleRestoreRun(${run.id})"
                                    class="inline-flex items-center px-2 py-1 text-xs border border-green-500 text-green-700 hover:bg-green-50 transition-colors"
                                    title="Restore run">
                                <i class="bi bi-arrow-counterclockwise"></i>
                            </button>
                            <a href="/profile/view-report/health_check/${run.id}"
                               class="inline-flex items-center px-2 py-1 text-xs border border-blue-500 text-blue-700 hover:bg-blue-50 transition-colors"
                               title="View Report">
                                <i class="bi bi-eye"></i>
                            </a>
                        ` : `
                            <button class="favorite-btn text-lg"
                                    data-run-id="${run.id}"
                                    data-is-favorite="${run.is_favorite ? 'true' : 'false'}"
                                    title="${run.is_favorite ? 'Remove from favorites' : 'Add to favorites'}">
                                ${run.is_favorite ? '⭐' : '☆'}
                            </button>
                            <a href="/profile/view-report/health_check/${run.id}"
                               class="inline-flex items-center px-2 py-1 text-xs border border-blue-500 text-blue-700 hover:bg-blue-50 transition-colors"
                               title="View Report">
                                <i class="bi bi-eye"></i>
                            </a>
                            ${renderEditButton(run)}
                            ${renderDownloadButton(run)}
                        `}
                    </div>
                </td>
            </tr>
        `;
    }).join('');

    // Add event listeners to checkboxes
    tbody.querySelectorAll('.run-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', (e) => {
            const runId = parseInt(e.target.dataset.runId);
            if (e.target.checked) {
                state.selectedRunIds.add(runId);
            } else {
                state.selectedRunIds.delete(runId);
            }
            updateTableSelection();
            updateActionBar();
        });
    });

    // Add event listeners to favorite buttons
    tbody.querySelectorAll('.favorite-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            const runId = parseInt(e.target.dataset.runId);
            const isFavorite = e.target.dataset.isFavorite === 'true';
            await toggleFavorite(runId, !isFavorite);
        });
    });

    updateTableSelection();
}

// Pagination Rendering
function renderPagination() {
    const totalPages = Math.ceil(state.filteredRuns.length / state.pageSize);
    const controls = document.getElementById('pagination-controls');

    if (totalPages <= 1) {
        controls.innerHTML = '';
        updatePaginationInfo();
        return;
    }

    let html = '';

    // Previous button
    html += `
        <button class="px-3 py-1 text-sm border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
                ${state.currentPage === 1 ? 'disabled' : ''}
                onclick="goToPage(${state.currentPage - 1})">
            ← Prev
        </button>
    `;

    // Page numbers (show max 7 pages)
    const maxPages = 7;
    let startPage = Math.max(1, state.currentPage - Math.floor(maxPages / 2));
    let endPage = Math.min(totalPages, startPage + maxPages - 1);

    if (endPage - startPage < maxPages - 1) {
        startPage = Math.max(1, endPage - maxPages + 1);
    }

    if (startPage > 1) {
        html += `<button class="px-3 py-1 text-sm border border-slate-300 rounded hover:bg-slate-50" onclick="goToPage(1)">1</button>`;
        if (startPage > 2) {
            html += `<span class="px-2 text-slate-500">...</span>`;
        }
    }

    for (let i = startPage; i <= endPage; i++) {
        html += `
            <button class="px-3 py-1 text-sm border border-slate-300 rounded ${i === state.currentPage ? 'bg-blue-600 text-white' : 'hover:bg-slate-50'}"
                    onclick="goToPage(${i})">
                ${i}
            </button>
        `;
    }

    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            html += `<span class="px-2 text-slate-500">...</span>`;
        }
        html += `<button class="px-3 py-1 text-sm border border-slate-300 rounded hover:bg-slate-50" onclick="goToPage(${totalPages})">${totalPages}</button>`;
    }

    // Next button
    html += `
        <button class="px-3 py-1 text-sm border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
                ${state.currentPage === totalPages ? 'disabled' : ''}
                onclick="goToPage(${state.currentPage + 1})">
            Next →
        </button>
    `;

    controls.innerHTML = html;
    updatePaginationInfo();
}

function goToPage(page) {
    state.currentPage = page;
    renderTable();
    renderPagination();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function updatePaginationInfo() {
    const start = (state.currentPage - 1) * state.pageSize + 1;
    const end = Math.min(state.currentPage * state.pageSize, state.filteredRuns.length);
    const total = state.filteredRuns.length;

    document.getElementById('page-start').textContent = total === 0 ? 0 : start;
    document.getElementById('page-end').textContent = end;
    document.getElementById('total-runs').textContent = total;
}

// Selection Management
function updateTableSelection() {
    // Update individual row styling
    document.querySelectorAll('.run-checkbox').forEach(checkbox => {
        const row = checkbox.closest('tr');
        const runId = parseInt(checkbox.dataset.runId);

        if (state.selectedRunIds.has(runId)) {
            row.classList.add('selected');
            checkbox.checked = true;
        } else {
            row.classList.remove('selected');
            checkbox.checked = false;
        }
    });

    // Update select-all checkbox
    const visibleRunIds = state.filteredRuns.slice(
        (state.currentPage - 1) * state.pageSize,
        state.currentPage * state.pageSize
    ).map(run => run.id);

    const allVisible = visibleRunIds.length > 0 && visibleRunIds.every(id => state.selectedRunIds.has(id));
    document.getElementById('select-all').checked = allVisible;

    // Update selection count
    document.getElementById('selected-count').textContent = state.selectedRunIds.size;
}

function updateActionBar() {
    const actionBar = document.getElementById('action-bar');
    const count = state.selectedRunIds.size;

    if (count > 0) {
        actionBar.classList.remove('hidden');
        document.getElementById('action-bar-count').textContent = count;

        // Enable/disable compare button (need at least 2)
        const compareBtn = document.getElementById('compare-selected-btn');
        compareBtn.disabled = count < 2;
    } else {
        actionBar.classList.add('hidden');
    }
}

// Sort Indicators
function updateSortIndicators() {
    document.querySelectorAll('th.sortable').forEach(th => {
        const column = th.dataset.sort;
        const icon = th.querySelector('.sort-icon');

        if (column === state.sortColumn) {
            th.classList.add('active');
            icon.textContent = state.sortDirection === 'asc' ? '↑' : '↓';
        } else {
            th.classList.remove('active');
            icon.textContent = '↕';
        }
    });
}

// Actions
async function handleCompareSelected() {
    if (state.selectedRunIds.size < 2) {
        alert('Please select at least 2 runs to compare');
        return;
    }

    const runIds = Array.from(state.selectedRunIds).sort((a, b) => a - b);
    const url = `/compare?run_ids=${runIds.join(',')}`;

    // Open in new window
    const width = 1200;
    const height = 800;
    const left = (screen.width - width) / 2;
    const top = (screen.height - height) / 2;

    window.open(
        url,
        'CompareRuns',
        `width=${width},height=${height},left=${left},top=${top},scrollbars=yes,resizable=yes`
    );
}

async function handleBulkDelete() {
    if (state.selectedRunIds.size === 0) {
        alert('Please select at least one run to delete');
        return;
    }

    const count = state.selectedRunIds.size;
    const runIds = Array.from(state.selectedRunIds);

    const confirmed = confirm(
        `Are you sure you want to delete ${count} selected run${count > 1 ? 's' : ''}?\n\n` +
        `This action cannot be undone and will permanently remove:\n` +
        `- Health check data\n` +
        `- Associated findings\n` +
        `- Triggered rules\n\n` +
        `Click OK to proceed with deletion.`
    );

    if (!confirmed) {
        return;
    }

    try {
        // Delete runs one by one
        let successCount = 0;
        let failCount = 0;
        const errors = [];

        for (const runId of runIds) {
            try {
                const response = await fetch(`/api/delete-run/${runId}`, {
                    method: 'DELETE',
                    headers: {
                        'Content-Type': 'application/json'
                    }
                });

                if (response.ok) {
                    successCount++;
                    state.selectedRunIds.delete(runId);
                } else {
                    failCount++;
                    const errorData = await response.json();
                    errors.push(`Run ${runId}: ${errorData.error || 'Unknown error'}`);
                }
            } catch (error) {
                failCount++;
                errors.push(`Run ${runId}: ${error.message}`);
            }
        }

        // Show results
        if (successCount > 0) {
            alert(`Successfully deleted ${successCount} run${successCount > 1 ? 's' : ''}.${failCount > 0 ? `\n\nFailed to delete ${failCount} run${failCount > 1 ? 's' : ''}.` : ''}`);

            // Refresh the table
            await fetchRuns();
            updateTableSelection();
            updateActionBar();
        } else {
            alert(`Failed to delete all selected runs.\n\n${errors.join('\n')}`);
        }

    } catch (error) {
        console.error('Error deleting runs:', error);
        alert(`Failed to delete runs: ${error.message}`);
    }
}

async function handleGenerateAiReport() {
    if (state.selectedRunIds.size === 0) {
        alert('Please select at least one run');
        return;
    }

    if (state.selectedRunIds.size > 10) {
        alert('Please select no more than 10 runs for AI analysis');
        return;
    }

    const runIds = Array.from(state.selectedRunIds);

    try {
        // Fetch user's AI profiles
        const profilesResponse = await fetch('/api/user-ai-profiles');
        if (!profilesResponse.ok) {
            alert('Failed to load AI profiles. Please configure an AI profile first.');
            return;
        }

        const profilesData = await profilesResponse.json();
        if (!profilesData || profilesData.length === 0) {
            alert('No AI profiles found. Please create an AI profile in your profile settings first.');
            window.location.href = '/profile';
            return;
        }

        // Show modal
        showAiReportModal(runIds, profilesData);

    } catch (error) {
        console.error('Error loading AI profiles:', error);
        alert(`Failed to load AI profiles: ${error.message}`);
    }
}

async function showAiReportModal(runIds, profiles) {
    const modal = document.getElementById('generate-ai-modal');
    const profileSelect = document.getElementById('ai-profile-select');
    const reportNameInput = document.getElementById('report-name-input');
    const reportDescInput = document.getElementById('report-description-input');
    const runCountSpan = document.getElementById('modal-run-count');
    const confirmBtn = document.getElementById('confirm-generate-ai-btn');
    const loadingDiv = document.getElementById('ai-modal-loading');
    const errorDiv = document.getElementById('ai-modal-error');
    const errorMsg = document.getElementById('ai-modal-error-message');

    // Reset modal state
    loadingDiv.classList.add('hidden');
    errorDiv.classList.add('hidden');
    confirmBtn.disabled = false;

    // Hide token estimate initially
    document.getElementById('token-estimate-section').classList.add('hidden');

    // Set run count
    runCountSpan.textContent = runIds.length;

    // Populate AI profiles dropdown
    profileSelect.innerHTML = '<option value="">Select AI Profile...</option>';
    profiles.forEach(profile => {
        const option = document.createElement('option');
        option.value = profile.id;
        option.textContent = profile.name;
        profileSelect.appendChild(option);
    });

    // Set default values
    if (profiles.length > 0) {
        profileSelect.value = profiles[0].id;
    }

    const today = new Date().toLocaleDateString();
    reportNameInput.value = `Health Check Analysis - ${today}`;
    reportDescInput.value = `Dashboard bulk analysis of ${runIds.length} health check runs`;

    // Show modal
    modal.classList.remove('hidden');
    modal.classList.add('flex');

    // Set up close button handlers
    const closeModal = () => {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
    };

    document.getElementById('close-ai-modal-x').onclick = closeModal;
    document.getElementById('close-ai-modal-cancel').onclick = closeModal;

    // Close on background click
    modal.onclick = (e) => {
        if (e.target === modal) {
            closeModal();
        }
    };

    // Set up confirm button handler (remove old listeners by cloning)
    const newConfirmBtn = confirmBtn.cloneNode(true);
    confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);

    newConfirmBtn.addEventListener('click', async () => {
        await submitAiReportGeneration(runIds, modal);
    });

    // Listen for profile changes and re-estimate tokens
    profileSelect.addEventListener('change', async () => {
        const selectedProfileId = parseInt(profileSelect.value);
        if (selectedProfileId) {
            await fetchTokenEstimate(runIds, selectedProfileId);
        }
    });

    // Fetch initial token estimate with default profile
    if (profiles.length > 0) {
        await fetchTokenEstimate(runIds, profiles[0].id);
    }
}

async function fetchTokenEstimate(runIds, profileId = null) {
    try {
        const requestBody = { run_ids: runIds };
        if (profileId) {
            requestBody.profile_id = profileId;
        }

        const response = await fetch('/api/estimate-bulk-report-tokens', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            console.error('Failed to fetch token estimate');
            return;
        }

        const data = await response.json();

        // Update UI with token estimates
        document.getElementById('token-input').textContent = data.estimated_input_tokens.toLocaleString();
        document.getElementById('token-output').textContent = data.estimated_output_tokens.toLocaleString();
        document.getElementById('token-total').textContent = data.total_estimated_tokens.toLocaleString();

        // Display warnings
        const warningsDiv = document.getElementById('token-warnings');
        warningsDiv.innerHTML = '';

        if (data.warnings && data.warnings.length > 0) {
            data.warnings.forEach(warning => {
                const warningEl = document.createElement('div');
                warningEl.className = 'text-xs mt-2 p-2 rounded';

                if (warning.level === 'critical') {
                    warningEl.className += ' bg-red-50 text-red-800 border border-red-200';
                    warningEl.innerHTML = `<strong>⚠️ Critical:</strong> ${warning.message}`;
                } else if (warning.level === 'warning') {
                    warningEl.className += ' bg-orange-50 text-orange-800 border border-orange-200';
                    warningEl.innerHTML = `<strong>⚠️ Warning:</strong> ${warning.message}`;
                } else {
                    warningEl.className += ' bg-blue-50 text-blue-800 border border-blue-200';
                    warningEl.innerHTML = `<strong>ℹ️ Info:</strong> ${warning.message}`;
                }

                warningsDiv.appendChild(warningEl);
            });
        }

        // Color the token box based on warnings
        const tokenBox = document.getElementById('token-estimate-box');
        tokenBox.className = 'p-3 border rounded';

        if (data.warnings.some(w => w.level === 'critical')) {
            tokenBox.className += ' border-red-300 bg-red-50';
        } else if (data.warnings.some(w => w.level === 'warning')) {
            tokenBox.className += ' border-orange-300 bg-orange-50';
        } else {
            tokenBox.className += ' border-green-300 bg-green-50';
        }

        // Update provider-specific rate limit info
        const rateLimitDiv = document.getElementById('token-rate-limits');
        if (data.provider_info) {
            const provider = data.provider_info;
            let limitsHtml = `<div class="text-xs text-slate-500 mt-2"><div><strong>${provider.provider_name}</strong> limits:</div>`;

            if (provider.context_window) {
                limitsHtml += `<div>• Context window: ${provider.context_window.toLocaleString()} tokens</div>`;
            }
            if (provider.rpm) {
                limitsHtml += `<div>• ${provider.rpm} requests/minute</div>`;
            }
            if (provider.tpm) {
                limitsHtml += `<div>• ${provider.tpm.toLocaleString()} tokens/minute</div>`;
            }
            if (provider.rpd) {
                limitsHtml += `<div>• ${provider.rpd.toLocaleString()} requests/day</div>`;
            }
            if (provider.tier) {
                limitsHtml += `<div class="text-xs text-slate-400 mt-1">(${provider.tier} tier)</div>`;
            }
            limitsHtml += '</div>';

            rateLimitDiv.innerHTML = limitsHtml;
        } else {
            // Fallback for when no provider info available
            rateLimitDiv.innerHTML = '<div class="text-xs text-slate-500 mt-2">Provider rate limits not available</div>';
        }

        // Show the token estimate section
        document.getElementById('token-estimate-section').classList.remove('hidden');

    } catch (error) {
        console.error('Error fetching token estimate:', error);
        // Don't show error to user - token estimate is optional
    }
}

async function submitAiReportGeneration(runIds, modal) {
    const profileSelect = document.getElementById('ai-profile-select');
    const reportNameInput = document.getElementById('report-name-input');
    const reportDescInput = document.getElementById('report-description-input');
    const analysisStyleSelect = document.getElementById('analysis-style-select');
    const confirmBtn = document.getElementById('confirm-generate-ai-btn');
    const loadingDiv = document.getElementById('ai-modal-loading');
    const errorDiv = document.getElementById('ai-modal-error');
    const errorMsg = document.getElementById('ai-modal-error-message');

    // Validate inputs
    const profileId = parseInt(profileSelect.value);
    const reportName = reportNameInput.value.trim();
    const analysisStyle = analysisStyleSelect.value || 'default';

    if (!profileId) {
        errorMsg.textContent = 'Please select an AI profile';
        errorDiv.classList.remove('hidden');
        return;
    }

    if (!reportName) {
        errorMsg.textContent = 'Please enter a report name';
        errorDiv.classList.remove('hidden');
        return;
    }

    // Hide error, show loading
    errorDiv.classList.add('hidden');
    loadingDiv.classList.remove('hidden');
    confirmBtn.disabled = true;

    try {
        const response = await fetch('/api/generate-bulk-ai-report', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                run_ids: runIds,
                profile_id: profileId,
                report_name: reportName,
                report_description: reportDescInput.value.trim(),
                analysis_style: analysisStyle
            })
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to generate AI report');
        }

        const data = await response.json();

        // Hide generation modal
        modal.classList.add('hidden');
        modal.classList.remove('flex');

        // Show success modal
        const successModal = document.getElementById('ai-report-success-modal');
        document.getElementById('success-report-id').textContent = data.report_id;
        document.getElementById('success-runs-count').textContent = data.runs_analyzed;

        successModal.classList.remove('hidden');
        successModal.classList.add('flex');

        // Close handlers
        const closeSuccessModal = () => {
            successModal.classList.add('hidden');
            successModal.classList.remove('flex');
        };

        document.getElementById('close-success-modal-x').onclick = closeSuccessModal;

        // Download button
        document.getElementById('success-download-btn').onclick = () => {
            window.location.href = data.download_url;
        };

        // View history button
        document.getElementById('success-view-history-btn').onclick = () => {
            window.location.href = '/profile/report-history';
        };

    } catch (error) {
        console.error('Error generating AI report:', error);
        loadingDiv.classList.add('hidden');
        confirmBtn.disabled = false;
        errorMsg.textContent = error.message;
        errorDiv.classList.remove('hidden');
    }
}

async function toggleFavorite(runId, isFavorite) {
    try {
        const response = await fetch('/api/runs/toggle-favorite', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ run_id: runId, is_favorite: isFavorite })
        });

        if (!response.ok) throw new Error('Failed to toggle favorite');

        // Update local state
        const run = state.runs.find(r => r.id === runId);
        if (run) {
            run.is_favorite = isFavorite;
        }

        // Re-render if favorites filter is active
        if (state.filters.status === 'favorites') {
            applyFilters();
        } else {
            // Just update the icon
            const btn = document.querySelector(`button[data-run-id="${runId}"]`);
            if (btn) {
                btn.textContent = isFavorite ? '⭐' : '☆';
                btn.dataset.isFavorite = isFavorite;
                btn.title = isFavorite ? 'Remove from favorites' : 'Add to favorites';
            }
        }
    } catch (error) {
        console.error('Error toggling favorite:', error);
        alert('Failed to update favorite status');
    }
}

// Filter Persistence
async function handleSaveFilter() {
    const filterName = document.getElementById('filter-name-input').value.trim();

    if (!filterName) {
        alert('Please enter a filter name');
        return;
    }

    try {
        const response = await fetch('/api/filters/save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                screen: 'dashboard',
                filter_name: filterName,
                filter_values: state.filters,
                set_selected: true
            })
        });

        if (!response.ok) throw new Error('Failed to save filter');

        alert('Filter saved successfully');
        document.getElementById('save-filter-modal').classList.add('hidden');
        document.getElementById('save-filter-modal').classList.remove('flex');
        document.getElementById('filter-name-input').value = '';
    } catch (error) {
        console.error('Error saving filter:', error);
        alert('Failed to save filter. Please try again.');
    }
}

async function handleLoadFilter() {
    try {
        const response = await fetch('/api/filters/list?screen=dashboard');
        if (!response.ok) throw new Error('Failed to load filters');

        const data = await response.json();
        const filters = data.filters || [];

        if (filters.length === 0) {
            alert('No saved filters found');
            return;
        }

        // Show modal
        const modal = document.getElementById('load-filter-modal');
        const filterSelect = document.getElementById('filter-list-select');
        const confirmBtn = document.getElementById('confirm-load-filter-btn');

        // Populate filter list
        filterSelect.innerHTML = '';
        filters.forEach((filter, idx) => {
            const option = document.createElement('option');
            option.value = idx;
            option.textContent = `${filter.filter_name}${filter.filter_selected ? ' (current)' : ''}`;
            option.dataset.filterId = filter.filter_id;
            filterSelect.appendChild(option);
        });

        // Show modal
        modal.classList.remove('hidden');
        modal.classList.add('flex');

        // Close handlers
        const closeModal = () => {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
        };

        document.getElementById('close-load-filter-x').onclick = closeModal;
        document.getElementById('close-load-filter-cancel').onclick = closeModal;

        // Close on background click
        modal.onclick = (e) => {
            if (e.target === modal) closeModal();
        };

        // Load selected filter
        const loadSelectedFilter = async () => {
            const selectedIndex = filterSelect.selectedIndex;
            if (selectedIndex < 0) return;

            const filter = filters[selectedIndex];
            await applyFilterFromData(filter);
            closeModal();
        };

        // Double-click to load
        filterSelect.ondblclick = loadSelectedFilter;

        // Confirm button
        const newConfirmBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
        newConfirmBtn.onclick = loadSelectedFilter;

    } catch (error) {
        console.error('Error loading filters:', error);
        alert('Failed to load filters. Please try again.');
    }
}

async function applyFilterFromData(filter) {
    // Apply filter values
    Object.assign(state.filters, filter.filter_values);

    // Update UI
    document.getElementById('company-filter').value = state.filters.company || '';

    // Populate targets based on company selection before setting target value
    populateTargetFilter();

    document.getElementById('target-filter').value = state.filters.target || '';
    document.getElementById('timerange-filter').value = state.filters.timerange || '30d';
    document.getElementById('technology-filter').value = state.filters.technology || '';
    document.getElementById('status-filter').value = state.filters.status || '';
    document.getElementById('search-filter').value = state.filters.search || '';

    if (state.filters.timerange === 'custom') {
        document.getElementById('custom-dates').classList.remove('hidden');
        document.getElementById('start-date').value = state.filters.startDate || '';
        document.getElementById('end-date').value = state.filters.endDate || '';
    }

    // Set as selected filter
    await fetch('/api/filters/set-selected', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            screen: 'dashboard',
            filter_id: filter.filter_id
        })
    });

    applyFilters();
}

async function loadSelectedFilter() {
    try {
        const response = await fetch('/api/filters/selected?screen=dashboard');
        if (!response.ok) return; // No selected filter is fine

        const data = await response.json();
        if (data.filter && data.filter.filter_values) {
            // Apply saved filter
            Object.assign(state.filters, data.filter.filter_values);

            // Update UI
            document.getElementById('company-filter').value = state.filters.company || '';

            // Populate targets based on company selection before setting target value
            populateTargetFilter();

            document.getElementById('target-filter').value = state.filters.target || '';
            document.getElementById('timerange-filter').value = state.filters.timerange || '30d';
            document.getElementById('technology-filter').value = state.filters.technology || '';
            document.getElementById('status-filter').value = state.filters.status || '';
            document.getElementById('search-filter').value = state.filters.search || '';

            if (state.filters.timerange === 'custom') {
                document.getElementById('custom-dates').classList.remove('hidden');
                document.getElementById('start-date').value = state.filters.startDate || '';
                document.getElementById('end-date').value = state.filters.endDate || '';
            }
        }
    } catch (error) {
        console.error('Error loading selected filter:', error);
        // Don't show alert, just use defaults
    }
}

// Utility Functions
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function escapeHtml(unsafe) {
    return (unsafe || '')
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function showError(message) {
    // TODO: Implement proper error notification system
    alert(message);
}

// Toggle showing deleted runs
function handleToggleDeleted() {
    state.showDeleted = !state.showDeleted;

    // Update button text and styling
    const button = document.getElementById('toggle-deleted-btn');
    const buttonText = document.getElementById('toggle-deleted-text');

    if (state.showDeleted) {
        buttonText.textContent = 'Hide Deleted';
        button.classList.remove('bg-amber-100', 'hover:bg-amber-200', 'text-amber-900', 'border-amber-300');
        button.classList.add('bg-red-100', 'hover:bg-red-200', 'text-red-900', 'border-red-300');
    } else {
        buttonText.textContent = 'Show Deleted';
        button.classList.remove('bg-red-100', 'hover:bg-red-200', 'text-red-900', 'border-red-300');
        button.classList.add('bg-amber-100', 'hover:bg-amber-200', 'text-amber-900', 'border-amber-300');
    }

    // Reload runs with deleted filter
    fetchRuns();
}

// Restore a deleted run
async function handleRestoreRun(runId) {
    const confirmed = confirm('Are you sure you want to restore this run?');
    if (!confirmed) return;

    try {
        const response = await fetch(`/api/restore-run/${runId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        const data = await response.json();

        if (response.ok) {
            alert(data.message || 'Run restored successfully');
            // Refresh the table
            await fetchRuns();
        } else {
            alert(data.error || 'Failed to restore run');
        }
    } catch (error) {
        console.error('Error restoring run:', error);
        alert(`Failed to restore run: ${error.message}`);
    }
}
