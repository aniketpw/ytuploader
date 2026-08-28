const fs = require('fs');
let code = fs.readFileSync('public/index.html', 'utf8');

// 1. Update the table header to move Date next to # (Serial No)
const oldThead = `<thead class="bg-slate-50 text-slate-700 border-b border-slate-200 font-bold tracking-tight">
            <tr>
              <th scope="col" class="py-3 px-3 w-12 text-center border-r border-slate-200">#</th>
              <th scope="col" class="py-3 px-4 min-w-[340px] border-r border-slate-200">
                File Name &amp; Batch
                <span class="block text-[10px] text-slate-400 font-normal lowercase">(click ✏️ to edit title live)</span>
              </th>
              <th scope="col" class="py-3 px-4 w-28 border-r border-slate-200 text-center">Date</th>
              <th scope="col" class="py-3 px-4 w-36 border-r border-slate-200">File Type</th>
              <th scope="col" class="py-3 px-4 w-80 border-r border-slate-200">
                Upload Status
                <span class="block text-[10px] text-slate-400 font-normal lowercase">(live progress &amp; speed)</span>
              </th>
              <th scope="col" class="py-3 px-4 w-36 text-center">Action</th>
            </tr>
          </thead>`;

const newThead = `<thead class="bg-slate-50 text-slate-700 border-b border-slate-200 font-bold tracking-tight">
            <tr>
              <th scope="col" class="py-3 px-3 w-12 text-center border-r border-slate-200">#</th>
              <th scope="col" class="py-3 px-4 w-32 border-r border-slate-200 text-center">Video Date</th>
              <th scope="col" class="py-3 px-4 min-w-[340px] border-r border-slate-200">
                File Name &amp; Batch
                <span class="block text-[10px] text-slate-400 font-normal lowercase">(click ✏️ to edit title live)</span>
              </th>
              <th scope="col" class="py-3 px-4 w-36 border-r border-slate-200">File Type</th>
              <th scope="col" class="py-3 px-4 w-80 border-r border-slate-200">
                Upload Status
                <span class="block text-[10px] text-slate-400 font-normal lowercase">(live progress &amp; speed)</span>
              </th>
              <th scope="col" class="py-3 px-4 w-36 text-center">Action</th>
            </tr>
          </thead>`;

if (code.includes(oldThead)) {
  code = code.replace(oldThead, newThead);
  console.log('✔ Replaced thead');
} else {
  console.log('⚠ Could not match oldThead directly');
}

// 2. Add Date Analytics & Multi-Date Filter Bar right above the table
const oldTableContainerStart = `    <!-- 4. Clean Enterprise Table -->
    <div class="bg-white border border-slate-200 rounded-lg shadow-2xs overflow-hidden">`;

const newDateFilterBar = `    <!-- Date Upload Tracker & Multi-Date Filter Bar -->
    <div id="dateFilterSection" class="bg-white p-3 rounded-lg border border-slate-200 shadow-2xs space-y-2.5">
      <div class="flex items-center justify-between flex-wrap gap-2">
        <div class="flex items-center gap-2">
          <span class="text-xs font-bold text-slate-800 uppercase tracking-wider flex items-center gap-1.5">
            <svg class="w-4 h-4 text-[#5046e5]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>
            </svg>
            Upload Date Breakdown &amp; Filters:
          </span>
          <span id="activeDateFilterLabel" class="text-[11px] text-slate-500 font-medium">(Showing all dates)</span>
        </div>

        <div class="flex items-center gap-2">
          <label class="inline-flex items-center gap-1.5 text-xs text-slate-600 font-semibold cursor-pointer select-none">
            <input type="checkbox" id="multiDateModeCheckbox" onchange="toggleMultiDateMode(this.checked)" class="rounded border-slate-300 text-[#5046e5] focus:ring-[#5046e5] cursor-pointer">
            <span>Multiple Dates Mode</span>
          </label>
          <button type="button" onclick="clearDateFilter()" class="text-xs text-rose-500 hover:text-rose-700 font-semibold underline cursor-pointer">
            Reset Filter
          </button>
        </div>
      </div>

      <!-- Quick Date Pills (Today, Yesterday, Specific Dates with Counters) -->
      <div id="dateChipsContainer" class="flex items-center flex-wrap gap-2">
        <button type="button" onclick="selectDateFilter('all')" id="dateChip-all" class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-bold bg-slate-900 text-white shadow-xs transition-all cursor-pointer">
          <span>All Dates</span>
          <span id="dateChipCount-all" class="px-1.5 py-0.2 rounded-full bg-white/20 text-[10px]">0</span>
        </button>
        
        <button type="button" onclick="selectDateFilter('today')" id="dateChip-today" class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-semibold bg-indigo-50 border border-indigo-200 text-[#5046e5] hover:bg-indigo-100 transition-all cursor-pointer">
          <span>Today (Aaj)</span>
          <span id="dateChipCount-today" class="px-1.5 py-0.2 rounded-full bg-indigo-200 text-indigo-900 text-[10px] font-bold">0</span>
        </button>

        <button type="button" onclick="selectDateFilter('yesterday')" id="dateChip-yesterday" class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-semibold bg-slate-50 border border-slate-200 text-slate-700 hover:bg-slate-100 transition-all cursor-pointer">
          <span>Yesterday (Kal)</span>
          <span id="dateChipCount-yesterday" class="px-1.5 py-0.2 rounded-full bg-slate-200 text-slate-800 text-[10px] font-bold">0</span>
        </button>

        <!-- Dynamic Date Pills populated automatically -->
        <div id="dynamicDateChips" class="flex items-center flex-wrap gap-2 inline-flex"></div>
      </div>
    </div>

    <!-- 4. Clean Enterprise Table -->
    <div class="bg-white border border-slate-200 rounded-lg shadow-2xs overflow-hidden">`;

code = code.replace(oldTableContainerStart, newDateFilterBar);
console.log('✔ Injected Date Filter Section');

fs.writeFileSync('public/index.html', code);
