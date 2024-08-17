import panel as pn
import param
from echopop.live.live_survey import LiveSurvey
from echopop.live.sql_methods import SQL
import echopop.live.live_visualizer as elv
from pathlib import Path

class EchopopPanel(param.Parameterized):
    panel0 = param.Parameter()
    panel1 = param.Parameter()
    panel2 = param.Parameter()
    
    def __init__(self, **params):
        super().__init__(**params)
        self.panel0 = pn.Column()
        self.panel1 = pn.Column()
        self.panel2 = pn.Column()

    @param.depends('panel0', 'panel1', 'panel2')
    def view(self):
        return {
            'gridded_population_estimates': self.panel0,
            'alongtrack_population_estimates': self.panel1,
            'length_weight_distributions': self.panel2
        }

panel_object = EchopopPanel()

def update_panel_from_db():
    print("Updating Visualizations")
    
    live_init_config_path = "/home/exouser/test/Pop/live_initialization_config.yml"
    live_file_config_path = "/home/exouser/test/Pop/live_survey_year_2019_config.yml"

    realtime_survey = LiveSurvey(live_file_config_path, live_init_config_path, verbose=True)

    grid_db = Path(realtime_survey.config["database"]["grid"])
    survey_data_db = Path('/home/exouser/database/acoustics.db')
    coast_db = grid_db
    projection = realtime_survey.config["geospatial"]["projection"]
    weight_table = SQL(realtime_survey.config["database"]["biology"], "select", 
                    table_name="length_weight_df")
    stratum_table = SQL(realtime_survey.config["database"]["biology"], "select", 
                        table_name="strata_summary_df")
    specimen_table = SQL(realtime_survey.config["database"]["biology"], "select", 
                        table_name="specimen_data_df")
    length_table = SQL(realtime_survey.config["database"]["biology"], "select", 
                    table_name="length_df")
    
    fig = elv.plot_livesurvey_grid(grid_db, projection, coast_db)
    fig1 = elv.plot_livesurvey_track(survey_data_db, projection, coast_db)
    fig2 = elv.plot_livesurvey_distributions(weight_table, stratum_table, specimen_table, length_table)
    
    panel_object.panel0[:] = [pn.panel(fig)]
    panel_object.panel1[:] = [pn.panel(fig1)]
    panel_object.panel2[:] = [pn.panel(fig2)]

# Periodically update the panel
pn.state.add_periodic_callback(update_panel_from_db, 600000)

update_panel_from_db()

# Serve the Panel app
pn.serve(panel_object.view(), port=1800, websocket_origin="*", admin=True, show=False)
