const fs = require('fs');
let code = fs.readFileSync('public/index.html', 'utf8');

// Insert the select box after the last pill
const privacyPillEnd = `Privacy - Unlisted (100.00%)
      </div>
    </div>`;

const injectHtml = `Privacy - Unlisted (100.00%)
      </div>
      
      <div class="ml-auto flex items-center gap-2">
        <span class="text-xs font-bold text-slate-500 uppercase tracking-wider">Date:</span>
        <select id="tableDateFilter" onchange="setTableDateFilter(this.value)" class="h-8 px-3 rounded border border-slate-300 bg-white text-slate-700 text-xs font-bold shadow-2xs focus:outline-none focus:border-[#5046e5] cursor-pointer">
          <option value="all">All Dates</option>
        </select>
      </div>
    </div>`;

code = code.replace(privacyPillEnd, injectHtml);

// Add JS logic
const jsVarsFind = `    let currentFilter = 'all'; // 'all' | 'completed' | 'pending' | 'failed'`;
const jsVarsReplace = `    let currentFilter = 'all'; // 'all' | 'completed' | 'pending' | 'failed'
    let currentDateFilter = 'all';`;
code = code.replace(jsVarsFind, jsVarsReplace);

const setFilterFind = `    window.setTableFilter = function(filter) {
      currentFilter = filter;
      updateFilterPillsUI();
      renderFilteredTable();
    };`;
const setFilterReplace = `    window.setTableFilter = function(filter) {
      currentFilter = filter;
      updateFilterPillsUI();
      renderFilteredTable();
    };
    
    window.setTableDateFilter = function(dateStr) {
      currentDateFilter = dateStr;
      renderFilteredTable();
    };`;
code = code.replace(setFilterFind, setFilterReplace);

const renderTableFind = `      let filteredFiles = currentState.files;
      if (currentFilter === 'completed') {`;
const renderTableReplace = `      let filteredFiles = currentState.files;
      if (currentDateFilter !== 'all') {
        filteredFiles = filteredFiles.filter(f => {
          const fileDate = f.createdTime ? new Date(f.createdTime).toLocaleDateString('en-GB') : new Date().toLocaleDateString('en-GB');
          return fileDate === currentDateFilter;
        });
      }
      
      if (currentFilter === 'completed') {`;
code = code.replace(renderTableFind, renderTableReplace);

const renderStateFind = `    function renderState(state) {
      if (!state) return;
      currentState = state;`;
const renderStateReplace = `    function renderState(state) {
      if (!state) return;
      currentState = state;
      
      // Populate Date Dropdown
      const dateSelect = document.getElementById('tableDateFilter');
      if (dateSelect && state.files && state.files.length > 0) {
        const uniqueDates = [...new Set(state.files.map(f => {
          return f.createdTime ? new Date(f.createdTime).toLocaleDateString('en-GB') : new Date().toLocaleDateString('en-GB');
        }))].sort();
        
        // Keep current selection if valid
        const currentVal = dateSelect.value;
        dateSelect.innerHTML = '<option value="all">All Dates</option>';
        uniqueDates.forEach(d => {
          const opt = document.createElement('option');
          opt.value = d;
          opt.textContent = d;
          dateSelect.appendChild(opt);
        });
        if (uniqueDates.includes(currentVal)) {
          dateSelect.value = currentVal;
        } else {
          currentDateFilter = 'all';
          dateSelect.value = 'all';
        }
      }
      `;
code = code.replace(renderStateFind, renderStateReplace);

fs.writeFileSync('public/index.html', code);
console.log('patched table date filter');
