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

def update_data_tables_from_db():
    print("Updating Visualizations")
    
    live_init_config_path = "/home/exouser/config/live_initialization_config.yml"
    live_file_config_path = "/home/exouser/config/live_survey_year_2019_config.yml"

    realtime_survey = LiveSurvey(live_init_config_path=live_init_config_path,
                                live_file_config_path=live_file_config_path, verbose=True)

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
    
    return(grid_db, survey_data_db, coast_db, projection, weight_table, stratum_table, specimen_table, length_table)
    

# updating all tables
grid_db, survey_data_db, coast_db, projection, weight_table, stratum_table, specimen_table, length_table = update_data_tables_from_db()


    # fig = elv.plot_livesurvey_grid(grid_db, projection, coast_db)
    # fig1 = elv.plot_livesurvey_track(survey_data_db, projection, coast_db)
    # fig2 = elv.plot_livesurvey_distributions(weight_table, stratum_table, specimen_table, length_table)

# wrapping plotting functions
def plot_livesurvey_grid():
    grid_db, survey_data_db, coast_db, projection, weight_table, stratum_table, specimen_table, length_table = update_data_tables_from_db()
    return(elv.plot_livesurvey_grid(grid_db, projection, coast_db))

def plot_livesurvey_track():
    grid_db, survey_data_db, coast_db, projection, weight_table, stratum_table, specimen_table, length_table = update_data_tables_from_db()
    return(elv.plot_livesurvey_track(survey_data_db, projection, coast_db))

def plot_livesurvey_distributions():
    grid_db, survey_data_db, coast_db, projection, weight_table, stratum_table, specimen_table, length_table = update_data_tables_from_db()
    return(elv.plot_livesurvey_distributions(weight_table, stratum_table, specimen_table, length_table))

#    panel_object.panel0[:] = [pn.panel(fig)]
#    panel_object.panel1[:] = [pn.panel(fig1)]
#    panel_object.panel2[:] = [pn.panel(fig2)]

update_button = pn.widgets.Button(name='Refresh data')

def update_plot(event=None):   
    return(plot_livesurvey_grid())

fig = pn.Column(update_button,
    pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
    )


update_button = pn.widgets.Button(name='Refresh data')

def update_plot(event=None):   
    return(plot_livesurvey_track())

fig1 = pn.Column(update_button,
    pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
    )

update_button = pn.widgets.Button(name='Refresh data')

def update_plot(event=None):   
    return(plot_livesurvey_distributions())

fig2 = pn.Column(update_button,
    pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
    )


panel_object.panel0[:] = [fig]
panel_object.panel1[:] = [fig1]
panel_object.panel2[:] = [(fig2)]

# Periodically update the panel
# pn.state.add_periodic_callback(update_panel_from_db, 600000)

# update_panel_from_db()

# Serve the Panel app
pn.serve(panel_object.view(), port=1802, websocket_origin="*", admin=True, show=False)
